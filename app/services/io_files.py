import io
import re
import pandas as pd
from fastapi import UploadFile, HTTPException

CNPJ_COL_CANDIDATES = {
    "cnpj", "cnpj_matriz", "documento", "doc", "cpf_cnpj", "cnpj/cpf", "inscricao"
}


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def _find_cnpj_column(df: pd.DataFrame) -> str:
    for c in df.columns:
        if c in CNPJ_COL_CANDIDATES:
            return c
    for c in df.columns:
        if "cnpj" in c:
            return c
    if len(df.columns) == 1:
        return df.columns[0]
    raise HTTPException(status_code=400, detail=f"Não achei coluna de CNPJ. Colunas: {list(df.columns)}")


def _guess_cnpj_column_by_content(df: pd.DataFrame) -> str | None:
    """Tenta adivinhar a coluna de CNPJ pelo conteúdo.

    Estratégia: para cada coluna, limpa para dígitos e conta quantos valores viram 14 dígitos.
    Se alguma coluna tiver pelo menos 1 CNPJ válido, escolhe a que tiver mais.
    """
    best_col = None
    best_score = 0
    for c in df.columns:
        try:
            s = df[c].apply(_clean_cnpj)
        except Exception:
            continue
        score = int(s.apply(_is_valid_14).sum())
        if score > best_score:
            best_score = score
            best_col = c
    return best_col if best_score > 0 else None


def _extract_first_cnpj_from_row(row: pd.Series) -> str:
    """Extrai o primeiro CNPJ (14 dígitos) encontrado em qualquer célula da linha."""
    try:
        text = " ".join(["" if v is None else str(v) for v in row.values])
    except Exception:
        text = str(row)

    # pega sequências de 14 dígitos (com ou sem pontuação)
    digits = re.sub(r"\D+", " ", text)
    for token in digits.split():
        if _is_valid_14(token):
            return token

    # fallback: procura padrão com pontuação típica
    m = re.search(r"\b\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}\b", text)
    if m:
        cnpj = _clean_cnpj(m.group(0))
        return cnpj

    return ""


def _clean_cnpj(value) -> str:
    if value is None:
        return ""
    return re.sub(r"\D+", "", str(value))


def _is_valid_14(cnpj: str) -> bool:
    return bool(re.fullmatch(r"\d{14}", cnpj or ""))


def read_input_file_to_df(file: UploadFile) -> pd.DataFrame:
    name = (file.filename or "").lower().strip()
    content = file.file.read()

    if not content:
        raise HTTPException(status_code=400, detail="Arquivo vazio.")

    # -------------------------
    # Leitura CSV
    # -------------------------
    if name.endswith(".csv"):
        try:
            df = pd.read_csv(io.BytesIO(content), dtype=str)
        except UnicodeDecodeError:
            df = pd.read_csv(io.BytesIO(content), encoding="latin-1", dtype=str)

    # -------------------------
    # Leitura XLSX
    # -------------------------
    elif name.endswith(".xlsx"):
        df = pd.read_excel(io.BytesIO(content), engine="openpyxl", dtype=str)

    # -------------------------
    # Leitura XLS (antigo)
    # -------------------------
    elif name.endswith(".xls"):
        def _try_read_xls(header: int | None = 0, sheet_name=0) -> pd.DataFrame:
            return pd.read_excel(
                io.BytesIO(content),
                engine="xlrd",
                dtype=str,
                header=header,
                sheet_name=sheet_name
            )

        # Estratégia: tenta algumas combinações comuns antes de desistir
        last_exc = None
        for sheet in (0, 1, None):
            for header in (0, 1, None):
                try:
                    df = _try_read_xls(header=header, sheet_name=sheet)
                    # Se veio vazio demais, tenta próximo
                    if df is None or df.shape[1] == 0:
                        continue
                    # Achou algo plausível
                    break
                except Exception as e:
                    last_exc = e
                    df = None
            if df is not None and df.shape[1] > 0:
                break

        if df is None or df.shape[1] == 0:
            # Mensagem clara (e prática)
            raise HTTPException(
                status_code=400,
                detail=(
                    "Não consegui ler seu arquivo .XLS (formato antigo). "
                    "Abra no LibreOffice/Excel e salve como .XLSX, depois envie novamente."
                ),
            )

        # Se deu erro de encoding específico, cai aqui também
        # (mas a mensagem acima já cobre)

    else:
        raise HTTPException(status_code=400, detail="Envie um arquivo CSV, XLSX ou XLS.")

    # -------------------------
    # Normalização + coluna cnpj
    # -------------------------
    df = _normalize_columns(df)

    if df.shape[1] == 0:
        raise HTTPException(status_code=400, detail="Arquivo sem colunas reconhecíveis.")

    # 1) tenta achar pelo nome (cnpj, documento, etc)
    col = None
    try:
        col = _find_cnpj_column(df)
    except HTTPException:
        col = None

    # 2) se não achou pelo nome, tenta adivinhar pelo conteúdo
    if col is None:
        col = _guess_cnpj_column_by_content(df)

    # 3) se ainda não achou, faz varredura linha-a-linha (CNPJ pode estar em qualquer coluna)
    if col is None:
        df["cnpj_input"] = df.apply(lambda r: _extract_first_cnpj_from_row(r), axis=1)
        df["cnpj"] = df["cnpj_input"].apply(_clean_cnpj)
        df["cnpj_valido"] = df["cnpj"].apply(_is_valid_14)
    else:
        # cria SEMPRE as colunas padrão no DF inteiro
        df["cnpj_input"] = df[col]
        df["cnpj"] = df[col].apply(_clean_cnpj)
        df["cnpj_valido"] = df["cnpj"].apply(_is_valid_14)

    # separa válidos/ inválidos
    df_validos = df[df["cnpj_valido"]].drop_duplicates(subset=["cnpj"]).copy()
    df_invalidos = df[~df["cnpj_valido"]].copy()

    # se não tiver válidos, devolve DF vazio, mas com colunas garantidas
    if df_validos.empty:
        df_empty = df.head(0).copy()
        df_empty.attrs["invalidos"] = df_invalidos
        return df_empty

    df_validos.attrs["invalidos"] = df_invalidos
    return df_validos


def build_output_bytes(df_out: pd.DataFrame, output: str) -> bytes:
    output = (output or "").lower().strip()

    # Garantia: CNPJ sempre como texto (14 dígitos) para não perder zeros à esquerda no Excel
    if df_out is not None and "cnpj" in df_out.columns:
        try:
            df_out = df_out.copy()
            df_out["cnpj"] = (
                df_out["cnpj"].astype(str).str.replace(r"\\D+", "", regex=True).str.zfill(14)
            )
        except Exception:
            pass

    if output == "csv":
        buffer = io.StringIO()
        df_out.to_csv(buffer, index=False)
        return buffer.getvalue().encode("utf-8")

    if output == "xlsx":
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df_out.to_excel(writer, index=False, sheet_name="resultado")
        return buffer.getvalue()

    raise HTTPException(status_code=400, detail="output deve ser csv ou xlsx")