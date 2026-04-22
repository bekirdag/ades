import json
from pathlib import Path
import zipfile
from xml.sax.saxutils import escape as xml_escape


def _excel_column_name(index: int) -> str:
    value = index + 1
    letters = ""
    while value > 0:
        value, remainder = divmod(value - 1, 26)
        letters = chr(ord("A") + remainder) + letters
    return letters


def _write_test_xlsx_workbook(
    path: Path,
    *,
    sheets: dict[str, list[list[str]]],
) -> None:
    shared_strings: list[str] = []
    shared_lookup: dict[str, int] = {}

    def shared_index(value: str) -> int:
        if value not in shared_lookup:
            shared_lookup[value] = len(shared_strings)
            shared_strings.append(value)
        return shared_lookup[value]

    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        worksheet_overrides: list[str] = []
        workbook_sheet_entries: list[str] = []
        workbook_rel_entries: list[str] = []
        for sheet_index, (sheet_name, rows) in enumerate(sheets.items(), start=1):
            worksheet_rows: list[str] = []
            max_columns = max((len(row) for row in rows), default=1)
            for row_index, row in enumerate(rows, start=1):
                cells: list[str] = []
                for column_index, raw_value in enumerate(row):
                    value = str(raw_value)
                    if not value:
                        continue
                    reference = f"{_excel_column_name(column_index)}{row_index}"
                    cells.append(
                        f'<c r="{reference}" t="s"><v>{shared_index(value)}</v></c>'
                    )
                worksheet_rows.append(f'<row r="{row_index}">{"".join(cells)}</row>')
            dimension = (
                f"A1:{_excel_column_name(max_columns - 1)}{max(len(rows), 1)}"
            )
            worksheet_xml = (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                f'<dimension ref="{dimension}"/>'
                f'<sheetData>{"".join(worksheet_rows)}</sheetData>'
                "</worksheet>"
            )
            archive.writestr(f"xl/worksheets/sheet{sheet_index}.xml", worksheet_xml)
            worksheet_overrides.append(
                (
                    '<Override '
                    f'PartName="/xl/worksheets/sheet{sheet_index}.xml" '
                    'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
                )
            )
            workbook_sheet_entries.append(
                (
                    f'<sheet name="{xml_escape(sheet_name)}" '
                    f'sheetId="{sheet_index}" r:id="rId{sheet_index}"/>'
                )
            )
            workbook_rel_entries.append(
                (
                    f'<Relationship Id="rId{sheet_index}" '
                    'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
                    f'Target="worksheets/sheet{sheet_index}.xml"/>'
                )
            )

        workbook_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            f'<sheets>{"".join(workbook_sheet_entries)}</sheets>'
            "</workbook>"
        )
        archive.writestr("xl/workbook.xml", workbook_xml)
        archive.writestr(
            "xl/_rels/workbook.xml.rels",
            (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                f'{"".join(workbook_rel_entries)}'
                '<Relationship Id="rIdSharedStrings" '
                'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" '
                'Target="sharedStrings.xml"/>'
                "</Relationships>"
            ),
        )
        archive.writestr(
            "_rels/.rels",
            (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                '<Relationship Id="rId1" '
                'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
                'Target="xl/workbook.xml"/>'
                "</Relationships>"
            ),
        )
        archive.writestr(
            "[Content_Types].xml",
            (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
                '<Default Extension="xml" ContentType="application/xml"/>'
                '<Override PartName="/xl/workbook.xml" '
                'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
                '<Override PartName="/xl/sharedStrings.xml" '
                'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>'
                f'{"".join(worksheet_overrides)}'
                "</Types>"
            ),
        )
        archive.writestr(
            "xl/sharedStrings.xml",
            (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
                f'count="{len(shared_strings)}" uniqueCount="{len(shared_strings)}">'
                + "".join(
                    f"<si><t>{xml_escape(value)}</t></si>" for value in shared_strings
                )
                + "</sst>"
            ),
        )


def create_finance_raw_snapshots(root: Path) -> dict[str, Path]:
    raw_dir = root / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    archives_root = root / "sec-archives"
    archives_root.mkdir(parents=True, exist_ok=True)

    sec_path = raw_dir / "sec-company_tickers.json"
    sec_path.write_text(
        json.dumps(
            {
                "0": {"cik_str": 320193, "ticker": "TICKA", "title": "Issuer Alpha Holdings"},
                "1": {"cik_str": 789019, "ticker": "TICKB", "title": "Issuer Beta Group"},
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    sec_submissions_path = raw_dir / "sec_submissions.zip"
    with zipfile.ZipFile(
        sec_submissions_path,
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        alpha_accession = "0000320193-24-000001"
        alpha_document = "proxy2024.htm"
        alpha_accession_dir = alpha_accession.replace("-", "")
        alpha_archive_dir = archives_root / "320193" / alpha_accession_dir
        alpha_archive_dir.mkdir(parents=True, exist_ok=True)
        (alpha_archive_dir / alpha_document).write_text(
            """
            <html>
              <body>
                <h1>Executive Officers</h1>
                <table>
                  <tr><td>Jane Doe</td><td>Chief Executive Officer</td></tr>
                  <tr><td>Mark Smith</td><td>Chief Financial Officer</td></tr>
                </table>
                <h2>Board of Directors</h2>
                <p>Mary Major, Independent Director</p>
              </body>
            </html>
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        archive.writestr(
            "CIK0000320193.json",
            json.dumps(
                {
                    "cik": "0000320193",
                    "name": "Issuer Alpha Holdings",
                    "tickers": ["TICKA"],
                    "exchanges": ["Exchange Alpha"],
                    "formerNames": [{"name": "Issuer Alpha Legacy"}],
                    "filings": {
                        "recent": {
                            "accessionNumber": [alpha_accession],
                            "filingDate": ["2024-03-01"],
                            "form": ["DEF 14A"],
                            "primaryDocument": [alpha_document],
                            "primaryDocDescription": ["Definitive Proxy Statement"],
                        }
                    },
                },
                indent=2,
            )
            + "\n",
        )
        beta_accession = "0000789019-24-000007"
        beta_document = "def14a.htm"
        beta_accession_dir = beta_accession.replace("-", "")
        beta_archive_dir = archives_root / "789019" / beta_accession_dir
        beta_archive_dir.mkdir(parents=True, exist_ok=True)
        (beta_archive_dir / beta_document).write_text(
            """
            <html>
              <body>
                <p>John Roe | Chief Financial Officer</p>
                <p>Lucy Stone | Portfolio Manager</p>
              </body>
            </html>
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        archive.writestr(
            "CIK0000789019.json",
            json.dumps(
                {
                    "cik": "0000789019",
                    "name": "Issuer Beta Group",
                    "tickers": ["TICKB"],
                    "exchanges": ["EXCHX"],
                    "formerNames": [{"name": "Issuer Beta Group"}],
                    "filings": {
                        "recent": {
                            "accessionNumber": [beta_accession],
                            "filingDate": ["2024-04-15"],
                            "form": ["DEF 14A"],
                            "primaryDocument": [beta_document],
                            "primaryDocDescription": ["Definitive Proxy Statement"],
                        }
                    },
                },
                indent=2,
            )
            + "\n",
        )

    sec_companyfacts_path = raw_dir / "sec_companyfacts.zip"
    with zipfile.ZipFile(
        sec_companyfacts_path,
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        archive.writestr(
            "CIK0000320193.json",
            json.dumps(
                {
                    "cik": 320193,
                    "entityName": "Issuer Alpha Holdings",
                },
                indent=2,
            )
            + "\n",
        )
        archive.writestr(
            "CIK0000789019.json",
            json.dumps(
                {
                    "cik": 789019,
                    "entityName": "Issuer Beta Group",
                },
                indent=2,
            )
            + "\n",
        )

    symbol_path = raw_dir / "nasdaq_symbols.txt"
    symbol_path.write_text(
        "\n".join(
            [
                "Symbol|Security Name|Test Issue",
                "TICKA|Issuer Alpha Holdings Common Stock|N",
                "TICKB|Issuer Beta Group Common Stock|N",
                "ZZZZ|Ignore Test Security|Y",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    other_listed_path = raw_dir / "otherlisted.txt"
    other_listed_path.write_text(
        "\n".join(
            [
                "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|EXCHX Symbol",
                "TICKC|Issuer Gamma Group Common Stock|N|TICKC|N|100|N|N",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    finance_people_path = raw_dir / "finance_people_entities.json"
    finance_people_path.write_text(
        json.dumps(
            {
                "entities": [
                    {
                        "canonical_text": "Jane Doe",
                        "aliases": ["Jane A. Doe"],
                        "employer_name": "Issuer Alpha Holdings",
                        "employer_ticker": "TICKA",
                        "employer_cik": "0000320193",
                        "role_title": "Chief Executive Officer",
                        "role_class": "executive_officer",
                        "source_form": "DEF 14A",
                        "source_name": "sec-proxy-people",
                        "source_id": "0000320193:jane-doe:def14a",
                    },
                    {
                        "canonical_text": "John Roe",
                        "aliases": ["John Q. Roe"],
                        "employer_name": "Issuer Beta Group",
                        "employer_ticker": "TICKB",
                        "employer_cik": "0000789019",
                        "role_title": "Chief Financial Officer",
                        "role_class": "executive_officer",
                        "source_form": "DEF 14A",
                        "source_name": "sec-proxy-people",
                        "source_id": "0000789019:john-roe:def14a",
                    },
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    curated_path = raw_dir / "curated_finance_entities.json"
    curated_path.write_text(
        json.dumps(
            {
                "entities": [
                    {
                        "entity_type": "exchange",
                        "canonical_text": "EXCHX",
                        "aliases": ["Exchange Alpha"],
                    },
                    {
                        "entity_type": "exchange",
                        "canonical_text": "EXCHY",
                        "aliases": ["Exchange Beta"],
                    },
                    {
                        "entity_type": "exchange",
                        "canonical_text": "EXCHZ",
                        "aliases": ["EXCHZ", "Exchange Gamma"],
                    },
                    {
                        "entity_type": "exchange",
                        "canonical_text": "EXCHQ",
                        "aliases": ["Exchange Delta"],
                    },
                    {
                        "entity_type": "exchange",
                        "canonical_text": "EXCHR",
                        "aliases": ["Exchange Epsilon"],
                    },
                    {
                        "entity_type": "exchange",
                        "canonical_text": "London Stock Exchange",
                        "aliases": ["LSE"],
                    },
                    {
                        "entity_type": "exchange",
                        "canonical_text": "CME",
                        "aliases": ["Chicago Mercantile Exchange"],
                    },
                    {
                        "entity_type": "market_index",
                        "canonical_text": "S&P 500",
                        "aliases": ["S&P 500 Index"],
                        "metadata": {"region": "us"},
                    },
                    {
                        "entity_type": "market_index",
                        "canonical_text": "Cboe Volatility Index",
                        "aliases": ["VIX"],
                    },
                    {
                        "entity_type": "commodity",
                        "canonical_text": "Brent crude",
                        "aliases": ["Brent crude oil"],
                    },
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    return {
        "sec_companies": sec_path,
        "sec_submissions": sec_submissions_path,
        "sec_companyfacts": sec_companyfacts_path,
        "symbol_directory": symbol_path,
        "other_listed": other_listed_path,
        "finance_people": finance_people_path,
        "archives_root": archives_root,
        "curated_entities": curated_path,
    }


def create_finance_remote_sources(root: Path) -> dict[str, str]:
    snapshots = create_finance_raw_snapshots(root)
    return {
        "sec_companies_url": snapshots["sec_companies"].resolve().as_uri(),
        "sec_submissions_url": snapshots["sec_submissions"].resolve().as_uri(),
        "sec_companyfacts_url": snapshots["sec_companyfacts"].resolve().as_uri(),
        "symbol_directory_url": snapshots["symbol_directory"].resolve().as_uri(),
        "other_listed_url": snapshots["other_listed"].resolve().as_uri(),
        "finance_people_url": snapshots["finance_people"].resolve().as_uri(),
        "finance_people_archive_base_url": snapshots["archives_root"].resolve().as_uri(),
    }


def create_finance_country_profiles(root: Path) -> dict[str, dict[str, object]]:
    root.mkdir(parents=True, exist_ok=True)
    finance_raw_snapshots = create_finance_raw_snapshots(root / "finance-raw")

    ar_regulator = root / "ar-cnv.html"
    ar_regulator.write_text(
        "<html><body>CNV landing page</body></html>\n",
        encoding="utf-8",
    )
    ar_aif = root / "ar-cnv-aif.html"
    ar_aif.write_text(
        "<html><body>AIF issuer filings landing page</body></html>\n",
        encoding="utf-8",
    )
    ar_byma = root / "ar-byma.html"
    ar_byma.write_text(
        "<html><body>BYMA landing page</body></html>\n",
        encoding="utf-8",
    )
    ar_byma_listed_companies = root / "ar-byma-listed-companies.html"
    ar_byma_listed_companies.write_text(
        (
            "<html><body>"
            '<a href="https://open.bymadata.com.ar/#/technical-detail-equity?symbol=YPFD" target="_blank" class="card-v1_header w-inline-block">'
            '<p class="text-color-primary text-weight-medium">YPFD</p>'
            '<h3 fs-cmssort-field="name" data-color="inherit" fs-cmsfilter-field="name" class="text-family-body text-size-medium">YPF S.A.</h3>'
            "</a>"
            '<div class="card_empresa-container"><div class="card_empresa-content">'
            '<div class="card_empresa-row"><div class="text-size-small text-color-gray">ISIN</div><div class="w-condition-invisible">-</div><div fs-cmsfilter-field="sector" class="text-size-small">ARP9897X1319</div></div>'
            '<div class="card_empresa-row"><div class="text-size-small text-color-gray">Sector</div><div fs-cmsfilter-field="sector" class="text-size-small">Energía</div></div>'
            '<div class="card_empresa-row"><div class="text-size-small text-color-gray">Domicilio</div><div fs-cmsfilter-field="sector" class="text-size-small">Macacha Güemes 515, CABA</div></div>'
            '<div class="api_card_footer"><div fs-cmsfilter-field="agent-type" class="text-size-tiny">S&amp;P BYMA General</div></div>'
            '<ul role="list" class="card_v1_call-to-actions"><li><a href="https://www.ypf.com/Paginas/home.aspx"></a></li><li><a href="https://open.bymadata.com.ar/#/technical-detail-equity?symbol=YPFD"></a></li></ul>'
            "</div></div>"
            '<a href="https://open.bymadata.com.ar/#/technical-detail-equity?symbol=GGAL" target="_blank" class="card-v1_header w-inline-block">'
            '<p class="text-color-primary text-weight-medium">GGAL</p>'
            '<h3 fs-cmssort-field="name" data-color="inherit" fs-cmsfilter-field="name" class="text-family-body text-size-medium">Grupo Financiero Galicia S.A.</h3>'
            "</a>"
            '<div class="card_empresa-container"><div class="card_empresa-content">'
            '<div class="card_empresa-row"><div class="text-size-small text-color-gray">ISIN</div><div class="w-condition-invisible">-</div><div fs-cmsfilter-field="sector" class="text-size-small">ARP495251018</div></div>'
            '<div class="card_empresa-row"><div class="text-size-small text-color-gray">Sector</div><div fs-cmsfilter-field="sector" class="text-size-small">Finanzas</div></div>'
            '<div class="card_empresa-row"><div class="text-size-small text-color-gray">Domicilio</div><div fs-cmsfilter-field="sector" class="text-size-small">Tte. Gral Juan Perón 430, CABA</div></div>'
            '<div class="api_card_footer"><div fs-cmsfilter-field="agent-type" class="text-size-tiny">S&amp;P Merval</div></div>'
            '<ul role="list" class="card_v1_call-to-actions"><li><a href="https://www.gfgsa.com/"></a></li><li><a href="https://open.bymadata.com.ar/#/technical-detail-equity?symbol=GGAL"></a></li></ul>'
            "</div></div>"
            '<a href="https://open.bymadata.com.ar/#/technical-detail-equity?symbol=BPAT" target="_blank" class="card-v1_header w-inline-block">'
            '<p class="text-color-primary text-weight-medium">BPAT</p>'
            '<h3 fs-cmssort-field="name" data-color="inherit" fs-cmsfilter-field="name" class="text-family-body text-size-medium">Banco Patagonia S.A.</h3>'
            "</a>"
            '<div class="card_empresa-container"><div class="card_empresa-content">'
            '<div class="card_empresa-row"><div class="text-size-small text-color-gray">ISIN</div><div class="w-condition-invisible">-</div><div fs-cmsfilter-field="sector" class="text-size-small">ARMERI013163</div></div>'
            '<div class="card_empresa-row"><div class="text-size-small text-color-gray">Sector</div><div fs-cmsfilter-field="sector" class="text-size-small">Finanzas</div></div>'
            '<div class="card_empresa-row"><div class="text-size-small text-color-gray">Domicilio</div><div fs-cmsfilter-field="sector" class="text-size-small">Avenida de Mayo 701, Piso 24, CABA</div></div>'
            '<div class="api_card_footer"><div fs-cmsfilter-field="agent-type" class="text-size-tiny">S&amp;P BYMA General</div></div>'
            '<ul role="list" class="card_v1_call-to-actions"><li><a href="https://www.bancopatagonia.com.ar/personas/index.php"></a></li><li><a href="https://open.bymadata.com.ar/#/technical-detail-equity?symbol=BPAT"></a></li></ul>'
            "</div></div>"
            '<a href="https://open.bymadata.com.ar/#/technical-detail-equity?symbol=CEPU2" target="_blank" class="card-v1_header w-inline-block">'
            '<p class="text-color-primary text-weight-medium">CEPU2</p>'
            '<h3 fs-cmssort-field="name" data-color="inherit" fs-cmsfilter-field="name" class="text-family-body text-size-medium">Central Puerto S.A.</h3>'
            "</a>"
            '<div class="card_empresa-container"><div class="card_empresa-content">'
            '<div class="card_empresa-row"><div class="text-size-small text-color-gray">ISIN</div><div class="w-condition-invisible">-</div><div fs-cmsfilter-field="sector" class="text-size-small">ARP2354Y1011</div></div>'
            '<div class="card_empresa-row"><div class="text-size-small text-color-gray">Sector</div><div fs-cmsfilter-field="sector" class="text-size-small">Utilities</div></div>'
            '<div class="card_empresa-row"><div class="text-size-small text-color-gray">Domicilio</div><div fs-cmsfilter-field="sector" class="text-size-small">Av. Thomas Edison 2701, CABA</div></div>'
            '<div class="api_card_footer"><div fs-cmsfilter-field="agent-type" class="text-size-tiny">S&amp;P Merval</div></div>'
            '<ul role="list" class="card_v1_call-to-actions"><li><a href="https://www.centralpuerto.com/"></a></li><li><a href="https://open.bymadata.com.ar/#/technical-detail-equity?symbol=CEPU2"></a></li></ul>'
            "</div></div>"
            "</body></html>\n"
        ),
        encoding="utf-8",
    )
    ar_cnv_emitter_regimes = root / "ar-cnv-emitter-regimes.xlsx"
    _write_test_xlsx_workbook(
        ar_cnv_emitter_regimes,
        sheets={
            "Sociedades": [
                ["Listado completo de sociedades por regimen"],
                ["Fecha descarga", "2026-04-20"],
                ["CUIT", "Sociedad", "Categoria", "Estado Categoria"],
                [
                    "30500006613",
                    "Banco Patagonia SA",
                    "Régimen General",
                    "En Oferta Pública",
                ],
                [
                    "30609506507",
                    "Central Puerto SA",
                    "Régimen General",
                    "En Oferta Pública",
                ],
                [
                    "30504963626",
                    "Grupo Financiero Galicia S.A.",
                    "Régimen General",
                    "En Oferta Pública",
                ],
                [
                    "30546689297",
                    "YPF Sociedad Anónima",
                    "Régimen General",
                    "En Oferta Pública",
                ],
            ]
        },
    )
    ar_bcra_financial_institutions_archive = (
        root / "ar-bcra-financial-institutions-202601d.7z"
    )
    ar_bcra_financial_institutions_archive.write_bytes(
        b"fake bcra 7z fixture\n",
    )
    ar_bcra_financial_institutions = root / "ar-bcra-financial-institutions.html"
    ar_bcra_financial_institutions.write_text(
        (
            "<html><body>"
            "<a href=\""
            f"{ar_bcra_financial_institutions_archive.resolve().as_uri()}"
            "\">202601d.7z</a>"
            "</body></html>\n"
        ),
        encoding="utf-8",
    )
    ar_bcra_entidad_completo = root / "ar-bcra-entidad-completo.txt"
    ar_bcra_entidad_completo.write_text(
        "\n".join(
            [
                '"00007"\t"BANCO DE GALICIA Y BUENOS AIRES S.A."\t"GALICIA Y BS AS"\t"GALICIA"\t"30-50000173-5"\t12\t""\t"Dic-2024"\t"Nov-2025"\t"Dic-2025"\t"Ene-2026"\t""\t"202412"\t"202511"\t"202512"\t"202601"\t""\t"Dic-2024"\t"Jun-2025"\t"Set-2025"\t"Dic-2025"\t""\t""\t""\t""\t""\t""\t"GRINENCO SERGIO"\t"GERENTE GENERAL"\t"RIVAS DIEGO"\t"TTE. GRAL. JUAN D. PERON 407"\t"Si"\t"CABA ZONA NORTE"\t"CABA"\t"080088842540"\t"3"\t"ADEBA"\t""\t""\t"Bancos Locales de Capital Nacional"\t"Casa Matriz"\t"AA121"\t"https://www.galicia.ar"\t"IG:@BancoGalicia"\t"relacionesinstitucionales@bancogalicia.com.ar"\t""\t""',
                '"00017"\t"BANCO BBVA ARGENTINA S.A."\t"BBVA ARGENTINA"\t"BBVA"\t"30-50001008-0"\t12\t""\t"Dic-2024"\t"Nov-2025"\t"Dic-2025"\t"Ene-2026"\t""\t"202412"\t"202511"\t"202512"\t"202601"\t""\t"Dic-2024"\t"Jun-2025"\t"Set-2025"\t"Dic-2025"\t""\t""\t""\t""\t""\t""\t"PAOLINI MARTIN"\t"GERENTE GENERAL"\t"TASAT RICARDO"\t"AV. CORDOBA 111"\t"Si"\t"CABA ZONA NORTE"\t"CABA"\t"0800-333-0303"\t"4"\t"ABA"\t""\t""\t"Bancos Locales de Capital Extranjero"\t"Casa Central"\t"AA123"\t"https://www.bbva.com.ar"\t"IG:@bbva_argentina"\t"proteccionalusuario-arg@bbva.com"\t""\t""',
            ]
        )
        + "\n",
        encoding="latin-1",
    )
    ar_bcra_direct_completo = root / "ar-bcra-direct-completo.txt"
    ar_bcra_direct_completo.write_text(
        "\n".join(
            [
                '"00007"\t"BANCO DE GALICIA Y BUENOS AIRES S.A."\t"202512"\t"GRINENCO SERGIO"\t101\t"PRESIDENTE"',
                '"00007"\t"BANCO DE GALICIA Y BUENOS AIRES S.A."\t"202512"\t"PANDO GUILLERMO JUAN"\t102\t"VICEPRESIDENTE"',
                '"00017"\t"BANCO BBVA ARGENTINA S.A."\t"202512"\t"ROJAS LEONARDO DANIEL"\t103\t"DIRECTOR TITULAR"',
            ]
        )
        + "\n",
        encoding="latin-1",
    )
    ar_bcra_gerent_completo = root / "ar-bcra-gerent-completo.txt"
    ar_bcra_gerent_completo.write_text(
        "\n".join(
            [
                '"00007"\t"BANCO DE GALICIA Y BUENOS AIRES S.A."\t"202512"\t"RIVAS DIEGO"\t401\t"GERENTE GENERAL"',
                '"00017"\t"BANCO BBVA ARGENTINA S.A."\t"202512"\t"FRANCIA GUERRERO BEATRIZ"\t402\t"RESP. DEL AREA DE CUMPLIMIENTO NORMATIVO"',
            ]
        )
        + "\n",
        encoding="latin-1",
    )
    ar_bcra_respclie_completo = root / "ar-bcra-respclie-completo.txt"
    ar_bcra_respclie_completo.write_text(
        "\n".join(
            [
                '"00007"\t"BANCO DE GALICIA Y BUENOS AIRES S.A."\t"202602"\t"Roizis Lautaro"\t"RESP. DE ATEN. A USUARIOS DE SS. FIN. TITULAR"\t"circulodeservicios.edb.bcra@bancogalicia.com.ar"\t"Tte Gral J D Peron 407 Piso:2"\t"6329-6578"',
                '"00017"\t"BANCO BBVA ARGENTINA S.A."\t"202602"\t"Francia Guerrero Beatriz"\t"RESPONSABLE DEL AREA DE CUMPLIMIENTO NORMATIVO"\t"proteccionalusuario-arg@bbva.com"\t"Cordoba 111 Piso:23"\t"800999-0303"',
            ]
        )
        + "\n",
        encoding="latin-1",
    )
    ar_bymadata_all_instrument_select = root / "ar-bymadata-all-instrument-select.json"
    ar_bymadata_all_instrument_select.write_text(
        json.dumps(
            [
                {
                    "oid": 1001,
                    "deleted": False,
                    "code": "YPFD",
                    "description": "EQUITYINSTRUMENT",
                },
                {
                    "oid": 1002,
                    "deleted": False,
                    "code": "GGAL",
                    "description": "EQUITYINSTRUMENT",
                },
                {
                    "oid": 1003,
                    "deleted": False,
                    "code": "BPAT",
                    "description": "EQUITYINSTRUMENT",
                },
                {
                    "oid": 1004,
                    "deleted": False,
                    "code": "CEPU2",
                    "description": "EQUITYINSTRUMENT",
                },
                {
                    "oid": 1005,
                    "deleted": False,
                    "code": "AYPF",
                    "description": "EQUITYINSTRUMENT",
                },
                {
                    "oid": 1006,
                    "deleted": False,
                    "code": "BONO1",
                    "description": "BONDINSTRUMENT",
                },
            ],
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    ar_bymadata_species_general_ypfd = root / "ar-bymadata-especies-general-ypfd.json"
    ar_bymadata_species_general_ypfd.write_text(
        json.dumps(
            {
                "data": [
                    {
                        "codigoIsin": "ARP9897X1319",
                        "tipoEspecie": "Acciones",
                        "descripcion": "Pesos",
                        "denominacion": 'Ordinarias Escriturales "D" (1 Voto)',
                        "lider": "Si",
                        "insType": "EQUITY",
                        "emisor": "YPF S.A.",
                    }
                ]
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    ar_bymadata_society_general_ypfd = root / "ar-bymadata-sociedades-general-ypfd.json"
    ar_bymadata_society_general_ypfd.write_text(
        json.dumps(
            {
                "data": [
                    {
                        "website": "www.ypf.com.ar",
                        "calle": "Macacha Güemes 515",
                        "dpto": "",
                        "piso": "",
                        "cierreEjercicio": "31/12",
                        "actividad": "Estudio, exploración y explotación de hidrocarburos.",
                        "pais": "Argentina",
                        "fechaConstitucion": "02 de Junio de 1977",
                        "localidad": "Ciudad Autónoma de Buenos Aires",
                        "provinciaEstado": "Capital Federal",
                        "telefono": "(54) 11 4329 2000",
                        "codPostal": "C1106BKK",
                        "email": "",
                    }
                ]
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    ar_bymadata_society_administration_ypfd = (
        root / "ar-bymadata-sociedades-administracion-ypfd.json"
    )
    ar_bymadata_society_administration_ypfd.write_text(
        json.dumps(
            {
                "data": [
                    {"cargo": "Presidente", "persona": "MARIN HORACIO DANIEL"},
                    {"cargo": "Director Titular", "persona": "OTTINO EDUARDO ALBERTO"},
                    {"cargo": "Contador Certificante Titular", "persona": "COHEN GUILLERMO DANIEL"},
                ]
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    ar_bymadata_society_responsables_ypfd = (
        root / "ar-bymadata-sociedades-responsables-ypfd.json"
    )
    ar_bymadata_society_responsables_ypfd.write_text(
        json.dumps(
            {
                "data": [
                    {
                        "responsableMercado": "CHUN, MARGARITA",
                        "cargo": "Titular",
                        "cargoEmpresa": "",
                        "fechaDesignacion": "2023-09-07 00:00:00.0",
                    }
                ]
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    ar_bymadata_species_general_ggal = root / "ar-bymadata-especies-general-ggal.json"
    ar_bymadata_species_general_ggal.write_text(
        json.dumps(
            {
                "data": [
                    {
                        "codigoIsin": "ARP495251018",
                        "tipoEspecie": "Acciones",
                        "descripcion": "Pesos",
                        "denominacion": 'Ordinarias Escriturales "B" (1 voto)',
                        "lider": "Si",
                        "insType": "EQUITY",
                        "emisor": "GRUPO FINANCIERO GALICIA S.A.",
                    }
                ]
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    ar_bymadata_society_general_ggal = root / "ar-bymadata-sociedades-general-ggal.json"
    ar_bymadata_society_general_ggal.write_text(
        json.dumps(
            {
                "data": [
                    {
                        "website": "www.gfgsa.com",
                        "calle": "Tte. Gral Juan Perón 430",
                        "dpto": "",
                        "piso": "25",
                        "cierreEjercicio": "31/12",
                        "actividad": "Financiera y de inversión.",
                        "pais": "Argentina",
                        "fechaConstitucion": "",
                        "localidad": "Ciudad Autónoma de Buenos Aires",
                        "provinciaEstado": "Capital Federal",
                        "telefono": "",
                        "codPostal": "C1038AAJ",
                        "email": "",
                    }
                ]
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    ar_bymadata_society_administration_ggal = (
        root / "ar-bymadata-sociedades-administracion-ggal.json"
    )
    ar_bymadata_society_administration_ggal.write_text(
        json.dumps(
            {
                "data": [
                    {"cargo": "Presidente", "persona": "ESCASANY EDUARDO J."},
                    {"cargo": "Vicepresidente", "persona": "GUTIERREZ PABLO"},
                    {"cargo": "Miemb.Tit.Comite De Auditoria", "persona": "ESTECHO CLAUDIA RAQUEL"},
                ]
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    ar_bymadata_society_responsables_ggal = (
        root / "ar-bymadata-sociedades-responsables-ggal.json"
    )
    ar_bymadata_society_responsables_ggal.write_text(
        json.dumps(
            {
                "data": [
                    {
                        "responsableMercado": "Pedemonte, Enrique",
                        "cargo": "Titular",
                        "cargoEmpresa": "",
                        "fechaDesignacion": "2013-09-03 00:00:00.0",
                    }
                ]
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    ar_bymadata_species_general_bpat = root / "ar-bymadata-especies-general-bpat.json"
    ar_bymadata_species_general_bpat.write_text(
        json.dumps(
            {
                "data": [
                    {
                        "codigoIsin": "ARMERI013163",
                        "tipoEspecie": "Acciones",
                        "descripcion": "Pesos",
                        "denominacion": 'Ordinarias Escriturales "B" (1 voto)',
                        "lider": "No",
                        "insType": "EQUITY",
                        "emisor": "BANCO PATAGONIA S.A.",
                    }
                ]
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    ar_bymadata_society_administration_bpat = (
        root / "ar-bymadata-sociedades-administracion-bpat.json"
    )
    ar_bymadata_society_administration_bpat.write_text(
        json.dumps(
            {
                "data": [
                    {
                        "cargo": "Presidente",
                        "persona": "PARRE DOS SANTOS OSWALDO",
                    },
                    {
                        "cargo": "Vicepresidente",
                        "persona": "TREJO JUAN MANUEL",
                    },
                ]
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    ar_bymadata_society_responsables_bpat = (
        root / "ar-bymadata-sociedades-responsables-bpat.json"
    )
    ar_bymadata_society_responsables_bpat.write_text(
        json.dumps(
            {
                "data": [
                    {
                        "responsableMercado": "FERREYRA, DIEGO ANDRES",
                        "cargo": "Titular",
                        "cargoEmpresa": "",
                        "fechaDesignacion": "2022-10-27 00:00:00.0",
                    }
                ]
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    ar_bymadata_species_general_cepu2 = root / "ar-bymadata-especies-general-cepu2.json"
    ar_bymadata_species_general_cepu2.write_text(
        json.dumps({"data": []}, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    ar_bymadata_society_general_cepu2 = root / "ar-bymadata-sociedades-general-cepu2.json"
    ar_bymadata_society_general_cepu2.write_text(
        json.dumps({"data": []}, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    ar_bymadata_society_administration_cepu2 = (
        root / "ar-bymadata-sociedades-administracion-cepu2.json"
    )
    ar_bymadata_society_administration_cepu2.write_text(
        json.dumps(
            {
                "data": [
                    {"cargo": "Presidente", "persona": "RECA OSVALDO ARTURO"},
                    {"cargo": "Vicepresidente", "persona": "RUETE MARTIN"},
                ]
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    ar_bymadata_society_responsables_cepu2 = (
        root / "ar-bymadata-sociedades-responsables-cepu2.json"
    )
    ar_bymadata_society_responsables_cepu2.write_text(
        json.dumps(
            {
                "data": [
                    {
                        "responsableMercado": "Pollice, Osvaldo",
                        "cargo": "Suplente",
                        "cargoEmpresa": "",
                        "fechaDesignacion": "2012-12-20 00:00:00.0",
                    }
                ]
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    us_regulator = root / "us-sec.html"
    us_regulator.write_text("<html><body>US SEC landing page</body></html>\n", encoding="utf-8")
    us_exchange = root / "us-nyse.html"
    us_exchange.write_text("<html><body>NYSE listing hub</body></html>\n", encoding="utf-8")

    uk_regulator = root / "uk-fca.html"
    uk_regulator.write_text("<html><body>UK FCA register landing page</body></html>\n", encoding="utf-8")
    uk_exchange = root / "uk-lse.html"
    uk_exchange.write_text("<html><body>London Stock Exchange news</body></html>\n", encoding="utf-8")

    au_regulator = root / "au-asic.html"
    au_regulator.write_text("<html><body>ASIC landing page</body></html>\n", encoding="utf-8")
    au_exchange = root / "au-asx.html"
    au_exchange.write_text("<html><body>ASX landing page</body></html>\n", encoding="utf-8")
    au_listed_companies = root / "au-asx-listed-companies.csv"
    au_listed_companies.write_text(
        "\n".join(
            [
                "Company name,ASX code,GICS industry group",
                "ASX Limited,ASX,Diversified Financials",
                "WiseTech Global Limited,WTC,Software",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    au_board = root / "au-asx-board.html"
    au_board.write_text(
        """
        <div class="bio__heading"><h3>Vicki Carter</h3><p>Independent, Non-Executive Director<br />BA (Social Sciences), GradDipMgmt, GAICD</p></div>
        <div class="bio__heading"><h3>David Clarke</h3><p>Independent, Non-Executive Director, Chair<br />LLB</p></div>
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    au_executive = root / "au-asx-executive-team.html"
    au_executive.write_text(
        """
        <div class="bio__heading"><h3>Helen Lofthouse</h3><p>Managing Director and CEO<br />BSc (Hons), GAICD</p></div>
        <div class="bio__heading"><h3>Darren Yip</h3><p>Group Executive, Markets and Listings<br />BComm</p></div>
        """.strip()
        + "\n",
        encoding="utf-8",
    )

    jp_fsa = root / "jp-fsa.html"
    jp_fsa.write_text(
        "<html><body>Financial Services Agency landing page</body></html>\n",
        encoding="utf-8",
    )
    jp_edinet = root / "jp-edinet.html"
    jp_edinet.write_text(
        "<html><body>EDINET filings landing page</body></html>\n",
        encoding="utf-8",
    )
    jp_jpx = root / "jp-jpx.html"
    jp_jpx.write_text(
        "<html><body>JPX English landing page</body></html>\n",
        encoding="utf-8",
    )
    jp_jpx_disclosure_gate = root / "jp-jpx-disclosure-gate.html"
    jp_jpx_disclosure_gate.write_text(
        "<html><body>JPX English Disclosure GATE landing page</body></html>\n",
        encoding="utf-8",
    )
    jp_jpx_english_disclosure_workbook = (
        root / "jp-jpx-english-disclosure-availability-202603.xlsx"
    )
    _write_test_xlsx_workbook(
        jp_jpx_english_disclosure_workbook,
        sheets={
            "English Disclosure": [
                ["Availability of English Disclosure Information by Listed Companies"],
                [],
                ["as of March 31, 2026"],
                [],
                [
                    "Code",
                    "Company Name",
                    "Market Segment",
                    "Sector",
                    "Market Capitalization (JPY mil.)",
                    "Corporate Governance Reports disclosure status",
                    "Annual Securities Reports disclosure status",
                    "IR Presentations disclosure status",
                    "Other English Documents disclosed on IR Website",
                    "IR Website English Links",
                ],
                [
                    "1301",
                    "KYOKUYO CO.,LTD.",
                    "Prime",
                    "Fishery, Agriculture and Forestry",
                    "86,409",
                    "Available",
                    "Available",
                    "Available",
                    "Factsheet",
                    "https://www.kyokuyo.co.jp/en/index.html",
                ],
                [
                    "130A",
                    "Veritas In Silico Inc.",
                    "Growth",
                    "Services",
                    "11,245",
                    "Available",
                    "Available",
                    "Available",
                    "",
                    "https://www.veritasinsilico.com/en/",
                ],
                [
                    "1332",
                    "Nissui Corporation",
                    "Prime",
                    "Fishery, Agriculture and Forestry",
                    "287,517",
                    "Available",
                    "Available",
                    "Available",
                    "Integrated report",
                    "https://www.nissui.co.jp/english/index.html",
                ],
            ]
        },
    )
    jp_jpx_english_disclosure_availability = (
        root / "jp-jpx-english-disclosure-availability.html"
    )
    jp_jpx_english_disclosure_availability.write_text(
        (
            "<html><body>"
            f'<a href="{jp_jpx_english_disclosure_workbook.resolve().as_uri()}">'
            "Download current monthly workbook</a>"
            "</body></html>\n"
        ),
        encoding="utf-8",
    )
    jp_jpx_listed_company_search = root / "jp-jpx-listed-company-search.html"
    jp_jpx_listed_company_search.write_text(
        "<html><body>JPX listed company search landing page</body></html>\n",
        encoding="utf-8",
    )
    jp_jpx_listed_company_search_results_page = (
        root / "jp-jpx-listed-company-search-results-page-1.html"
    )
    jp_jpx_listed_company_search_results_page.write_text(
        """
        <html><body>
          <form name="JJK020030Form" method="POST" action="/tseHpFront/JJK020030Action.do">
            <input type="hidden" name="Transition" value="Transition">
            <input type="hidden" name="lstDspPg" value="1">
            <input type="hidden" name="dspGs" value="200">
            <input type="hidden" name="souKnsu" value="3">
            <table>
              <tr height="50">
                <th>Code</th>
                <th>Issue name</th>
                <th>Market Segment</th>
                <th>Industry</th>
                <th colspan="2">Fiscal year-end</th>
                <th>Alerts, etc.</th>
                <th>Basic information</th>
                <th>Stock prices</th>
              </tr>
              <tr height="50">
                <td align="center">13010<input type="hidden" name="ccJjCrpSelKekkLst_st[0].eqMgrCd" value="13010"></td>
                <td align="center">KYOKUYO CO.,LTD.<input type="hidden" name="ccJjCrpSelKekkLst_st[0].eqMgrNm" value="KYOKUYO CO.,LTD."></td>
                <td align="center">Prime<input type="hidden" name="ccJjCrpSelKekkLst_st[0].szkbuNm" value="Prime"></td>
                <td align="center">Fishery, Agriculture &amp; Forestry<input type="hidden" name="ccJjCrpSelKekkLst_st[0].gyshDspNm" value="Fishery, Agriculture &amp; Forestry"></td>
                <td align="center" colspan="2">March<input type="hidden" name="ccJjCrpSelKekkLst_st[0].dspYuKssnKi" value="March"></td>
                <td align="center">&nbsp;</td>
                <td align="center"><input type="button" name="detail_button" value="Basic information" onclick="gotoBaseJh('13010', '1');" class="activeButton" /></td>
                <td align="center"><a target="_blank" href="https://quote.jpx.co.jp/jpxhp/main/index.aspx?F=e_stock_detail&amp;disptype=information&amp;qcode=1301" class="negativeLinkEn">Stock prices</a></td>
              </tr>
              <tr height="50">
                <td align="center">130A0<input type="hidden" name="ccJjCrpSelKekkLst_st[1].eqMgrCd" value="130A0"></td>
                <td align="center">VERITAS IN SILICO INC.<input type="hidden" name="ccJjCrpSelKekkLst_st[1].eqMgrNm" value="VERITAS IN SILICO INC."></td>
                <td align="center">Growth<input type="hidden" name="ccJjCrpSelKekkLst_st[1].szkbuNm" value="Growth"></td>
                <td align="center">Services<input type="hidden" name="ccJjCrpSelKekkLst_st[1].gyshDspNm" value="Services"></td>
                <td align="center" colspan="2">December<input type="hidden" name="ccJjCrpSelKekkLst_st[1].dspYuKssnKi" value="December"></td>
                <td align="center">&nbsp;</td>
                <td align="center"><input type="button" name="detail_button" value="Basic information" onclick="gotoBaseJh('130A0', '1');" class="activeButton" /></td>
                <td align="center"><a target="_blank" href="https://quote.jpx.co.jp/jpxhp/main/index.aspx?F=e_stock_detail&amp;disptype=information&amp;qcode=130A" class="negativeLinkEn">Stock prices</a></td>
              </tr>
              <tr height="50">
                <td align="center">72030<input type="hidden" name="ccJjCrpSelKekkLst_st[2].eqMgrCd" value="72030"></td>
                <td align="center">TOYOTA MOTOR CORPORATION<input type="hidden" name="ccJjCrpSelKekkLst_st[2].eqMgrNm" value="TOYOTA MOTOR CORPORATION"></td>
                <td align="center">Prime<input type="hidden" name="ccJjCrpSelKekkLst_st[2].szkbuNm" value="Prime"></td>
                <td align="center">Transportation Equipment<input type="hidden" name="ccJjCrpSelKekkLst_st[2].gyshDspNm" value="Transportation Equipment"></td>
                <td align="center" colspan="2">March<input type="hidden" name="ccJjCrpSelKekkLst_st[2].dspYuKssnKi" value="March"></td>
                <td align="center">&nbsp;</td>
                <td align="center"><input type="button" name="detail_button" value="Basic information" onclick="gotoBaseJh('72030', '1');" class="activeButton" /></td>
                <td align="center"><a target="_blank" href="https://quote.jpx.co.jp/jpxhp/main/index.aspx?F=e_stock_detail&amp;disptype=information&amp;qcode=7203" class="negativeLinkEn">Stock prices</a></td>
              </tr>
            </table>
          </form>
        </body></html>
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    jp_jpx_listed_company_detail_1301 = root / "jp-jpx-listed-company-detail-1301.html"
    jp_jpx_listed_company_detail_1301.write_text(
        """
        <html><body>
          <form name="JJK020040Form" method="POST" action="/tseHpFront/JJK020030Action.do">
            <h3 class="fontsizeM">KYOKUYO CO.,LTD.</h3>
            <table>
              <tr>
                <th>Code</th>
                <th>ISIN Code</th>
                <th>Trading unit</th>
                <th>Date of incorporation</th>
              </tr>
              <tr>
                <td>13010</td>
                <td>JP3256800007</td>
                <td>100 shares</td>
                <td>1937/09/03</td>
              </tr>
              <tr>
                <th>Address of main office</th>
                <th>Listed exchange</th>
                <th>Date of listing</th>
                <th>Title of representative</th>
              </tr>
              <tr>
                <td>3-2-20 Akasaka, Minato-ku, Tokyo</td>
                <td>Tokyo Stock Exchange</td>
                <td>1949/05/16</td>
                <td>Representative Director and President</td>
              </tr>
              <tr>
                <th>Name of representative</th>
                <th>Investment unit as of the end of last month</th>
                <th>Earnings results announcement (scheduled)</th>
                <th>First quarter (scheduled)</th>
              </tr>
              <tr>
                <td>Tetsuya Inoue</td>
                <td>415,300</td>
                <td>2026/05/13</td>
                <td>2026/08/12</td>
              </tr>
            </table>
            <table>
              <tr>
                <th>Second quarter (scheduled)</th>
                <th>Third quarter (scheduled)</th>
                <th>Date of general shareholders meeting (scheduled)</th>
                <th>No. of listed shares</th>
              </tr>
              <tr>
                <td>2026/11/11</td>
                <td>2027/02/10</td>
                <td>2026/06/24</td>
                <td>54,000,000</td>
              </tr>
              <tr>
                <th>No. of issued shares</th>
                <th>Registration with J-IRISS</th>
                <th>Loan issue</th>
                <th>Margin issue</th>
              </tr>
              <tr>
                <td>54,000,000</td>
                <td>Registered</td>
                <td>Eligible</td>
                <td>Eligible</td>
              </tr>
              <tr>
                <th>Membership of Financial Accounting Standards Foundation</th>
                <th>Notes on going concern assumption</th>
                <th>Information on controlling shareholder(s), etc.</th>
              </tr>
              <tr>
                <td>Member</td>
                <td>None</td>
                <td>None</td>
              </tr>
            </table>
          </form>
        </body></html>
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    jp_jpx_listed_company_detail_1332 = root / "jp-jpx-listed-company-detail-1332.html"
    jp_jpx_listed_company_detail_1332.write_text(
        """
        <html><body>
          <form name="JJK020040Form" method="POST" action="/tseHpFront/JJK020030Action.do">
            <h3 class="fontsizeM">Nissui Corporation</h3>
            <table>
              <tr>
                <th>Code</th>
                <th>ISIN Code</th>
                <th>Trading unit</th>
                <th>Date of incorporation</th>
              </tr>
              <tr>
                <td>13320</td>
                <td>JP3718800000</td>
                <td>100 shares</td>
                <td>1943/03/31</td>
              </tr>
              <tr>
                <th>Address of main office</th>
                <th>Listed exchange</th>
                <th>Date of listing</th>
                <th>Title of representative</th>
              </tr>
              <tr>
                <td>1-6-1 Otemachi, Chiyoda-ku, Tokyo</td>
                <td>Tokyo Stock Exchange</td>
                <td>1949/05/16</td>
                <td>Representative Director and President</td>
              </tr>
              <tr>
                <th>Name of representative</th>
                <th>Investment unit as of the end of last month</th>
                <th>Earnings results announcement (scheduled)</th>
                <th>First quarter (scheduled)</th>
              </tr>
              <tr>
                <td>Teru Tanaka</td>
                <td>782,100</td>
                <td>2026/05/15</td>
                <td>2026/08/07</td>
              </tr>
            </table>
            <table>
              <tr>
                <th>Second quarter (scheduled)</th>
                <th>Third quarter (scheduled)</th>
                <th>Date of general shareholders meeting (scheduled)</th>
                <th>No. of listed shares</th>
              </tr>
              <tr>
                <td>2026/11/06</td>
                <td>2027/02/05</td>
                <td>2026/06/26</td>
                <td>312,430,277</td>
              </tr>
              <tr>
                <th>No. of issued shares</th>
                <th>Registration with J-IRISS</th>
                <th>Loan issue</th>
                <th>Margin issue</th>
              </tr>
              <tr>
                <td>312,430,277</td>
                <td>Registered</td>
                <td>Eligible</td>
                <td>Eligible</td>
              </tr>
              <tr>
                <th>Membership of Financial Accounting Standards Foundation</th>
                <th>Notes on going concern assumption</th>
                <th>Information on controlling shareholder(s), etc.</th>
              </tr>
              <tr>
                <td>Member</td>
                <td>None</td>
                <td>None</td>
              </tr>
            </table>
          </form>
        </body></html>
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    jp_jpx_listed_company_detail_130a = root / "jp-jpx-listed-company-detail-130a.html"
    jp_jpx_listed_company_detail_130a.write_text(
        """
        <html><body>
          <form name="JJK020040Form" method="POST" action="/tseHpFront/JJK020030Action.do">
            <h3 class="fontsizeM">Veritas In Silico Inc.</h3>
            <table>
              <tr>
                <th>Code</th>
                <th>ISIN Code</th>
                <th>Trading unit</th>
                <th>Date of incorporation</th>
              </tr>
              <tr>
                <td>130A0</td>
                <td>JP0000001306</td>
                <td>100 shares</td>
                <td>2024/01/22</td>
              </tr>
              <tr>
                <th>Address of main office</th>
                <th>Listed exchange</th>
                <th>Date of listing</th>
                <th>Title of representative</th>
              </tr>
              <tr>
                <td>2-1-1 Nihonbashi, Chuo-ku, Tokyo</td>
                <td>Tokyo Stock Exchange</td>
                <td>2024/03/21</td>
                <td>Representative Director and CEO</td>
              </tr>
              <tr>
                <th>Name of representative</th>
                <th>Investment unit as of the end of last month</th>
                <th>Earnings results announcement (scheduled)</th>
                <th>First quarter (scheduled)</th>
              </tr>
              <tr>
                <td>Atsushi Nakamura</td>
                <td>128,400</td>
                <td>2026/05/14</td>
                <td>2026/08/13</td>
              </tr>
            </table>
          </form>
        </body></html>
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    jp_jpx_corporate_governance_search = (
        root / "jp-jpx-corporate-governance-search.html"
    )
    jp_jpx_corporate_governance_search.write_text(
        "<html><body>JPX corporate governance search landing page</body></html>\n",
        encoding="utf-8",
    )
    jp_jpx_corporate_governance_results_page = (
        root / "jp-jpx-corporate-governance-results-page-1.html"
    )
    jp_jpx_corporate_governance_results_page.write_text(
        """
        <html><body>
          <form name="CGK020030Form" method="POST" action="/tseHpFront/CGK020030Action.do">
            <input type="hidden" name="Transition" value="Transition">
            <input type="hidden" name="lstDspPg" value="1">
            <input type="hidden" name="dspGs" value="100">
            <input type="hidden" name="souKnsu" value="2">
            <table>
              <tr>
                <td><a href="javascript:codeLink('13010');" class="txtLink">13010<input type="hidden" name="ccEibnCGSelKekkLst_st[0].eqMgrCd" value="13010"></a></td>
                <td>KYOKUYO CO.,LTD.<input type="hidden" name="ccEibnCGSelKekkLst_st[0].eqMgrNm" value="KYOKUYO CO.,LTD."></td>
                <td>2026/03/31<input type="hidden" name="ccEibnCGSelKekkLst_st[0].jhUpdDay" value="2026/03/31"></td>
                <td><a href="/disc/13010/140120260331123456.pdf" target="linkWin7_1" class="txtLink"><span class="fontsizeS">Corporate Governance Report</span></a></td>
              </tr>
              <tr>
                <td><a href="javascript:codeLink('13320');" class="txtLink">13320<input type="hidden" name="ccEibnCGSelKekkLst_st[1].eqMgrCd" value="13320"></a></td>
                <td>Nissui Corporation<input type="hidden" name="ccEibnCGSelKekkLst_st[1].eqMgrNm" value="Nissui Corporation"></td>
                <td>2025/09/03<input type="hidden" name="ccEibnCGSelKekkLst_st[1].jhUpdDay" value="2025/09/03"></td>
                <td><a href="/disc/13320/140120250903552493.pdf" target="linkWin7_1" class="txtLink"><span class="fontsizeS">[Delayed] Corporate Governance Report (June 26,2025)</span></a></td>
              </tr>
            </table>
          </form>
        </body></html>
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    jp_jpx_corporate_governance_report_1301 = (
        root / "jp-jpx-corporate-governance-report-1301.txt"
    )
    jp_jpx_corporate_governance_report_1301.write_text(
        """
        Corporate Governance Report
        Last Update: March 31, 2026

        Kyokuyo Co., Ltd.

        Tetsuya Inoue
        Representative Director and President
        Contact: Haruka Sato, Investor Relations Office
        Phone: +81-3-0000-0000
        Securities Code: 1301
        https://www.kyokuyo.co.jp/en/index.html
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    jp_jpx_corporate_governance_report_1332 = (
        root / "jp-jpx-corporate-governance-report-1332.txt"
    )
    jp_jpx_corporate_governance_report_1332.write_text(
        """
        Corporate Governance Report
        Last Update: June 26, 2025

        Nissui Corporation

        Teru Tanaka
        Representative Director and President
        Contact: Kunihiko Umemura, Investor Relations Section, Corporate Strategic Planning & IR Department
        Phone: +81-3-6206-7037
        Securities Code: 1332
        https://www.nissui.co.jp/english/index.html
        """.strip()
        + "\n",
        encoding="utf-8",
    )

    fr_amf = root / "fr-amf-listed-companies-issuers.html"
    fr_amf.write_text(
        "<html><body>AMF listed companies and issuers space</body></html>\n",
        encoding="utf-8",
    )
    fr_euronext_regulated = root / "fr-euronext-paris-regulated.html"
    fr_euronext_regulated.write_text(
        """
        <html>
          <body>
            <table>
              <tbody>
                <tr class="odd">
                  <td class="stocks-logo sorting_1"></td>
                  <td class="stocks-shortName" data-order="74SOFTWARE">
                    <a href="/en/product/equities/FR0011040500-XPAR" data-order="74SOFTWARE" data-title-hover="74SOFTWARE">74SOFTWARE</a>
                  </td>
                  <td class="stocks-isin" style="display: none;">FR0011040500</td>
                  <td class="stocks-symbol">74SW</td>
                  <td class="stocks-market" style="display: none;"><div class="nowrap pointer" title="Euronext Paris">XPAR</div></td>
                </tr>
                <tr class="odd">
                  <td class="stocks-logo sorting_1"></td>
                  <td class="stocks-shortName" data-order="ROCHE BOBOIS">
                    <a href="/en/product/equities/FR0013344173-XPAR" data-order="ROCHE BOBOIS" data-title-hover="ROCHE BOBOIS">ROCHE BOBOIS</a>
                  </td>
                  <td class="stocks-isin" style="display: none;">FR0013344173</td>
                  <td class="stocks-symbol">RBO</td>
                  <td class="stocks-market" style="display: none;"><div class="nowrap pointer" title="Euronext Paris">XPAR</div></td>
                </tr>
              </tbody>
            </table>
          </body>
        </html>
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    fr_euronext_growth = root / "fr-euronext-paris-growth.html"
    fr_euronext_growth.write_text(
        """
        <html>
          <body>
            <table>
              <tbody>
                <tr class="odd">
                  <td class="stocks-logo sorting_1"></td>
                  <td class="stocks-shortName" data-order="2CRSI">
                    <a href="/en/product/equities/FR0013341781-ALXP" data-order="2CRSI" data-title-hover="2CRSI">2CRSI</a>
                  </td>
                  <td class="stocks-isin" style="display: none;">FR0013341781</td>
                  <td class="stocks-symbol">AL2SI</td>
                  <td class="stocks-market"><div class="nowrap pointer" title="Euronext Growth Paris">ALXP</div></td>
                </tr>
              </tbody>
            </table>
          </body>
        </html>
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    fr_euronext_access = root / "fr-euronext-paris-access.html"
    fr_euronext_access.write_text(
        """
        <html>
          <body>
            <table>
              <tbody>
                <tr class="odd">
                  <td class="stocks-logo sorting_1"></td>
                  <td class="stocks-shortName" data-order="ACTIVIUM GROUP">
                    <a href="/en/product/equities/FR0010979377-XMLI" data-order="ACTIVIUM GROUP" data-title-hover="ACTIVIUM GROUP">ACTIVIUM GROUP</a>
                  </td>
                  <td class="stocks-isin" style="display: none;">FR0010979377</td>
                  <td class="stocks-symbol">MLACT</td>
                  <td class="stocks-market"><div class="nowrap pointer" title="Euronext Access Paris">XMLI</div></td>
                </tr>
              </tbody>
            </table>
          </body>
        </html>
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    fr_euronext_company_news = root / "fr-euronext-paris-company-news.html"
    fr_euronext_company_news.write_text(
        """
        <html>
          <body>
            <table>
              <tbody>
                <tr>
                  <td class="rawmap views-field pl-0 views-field-field-company-pr-pub-datetime"><span class='nowrap'> 20 Apr 2026</span> <br><span class='nowrap'> 19:00 CEST</span></td>
                  <td class="views-field pl-0 views-field-field-company-name">ROCHE BOBOIS</td>
                  <td class="views-field pl-0 views-field-title"><a href="" class="standardRightCompanyPressRelease" data-node-nid="12878420" data-toggle="modal" data-target="#standardRightCompanyPressRelease">Mise à disposition du Document d&#039;enregistrement universel au titre de l&#039;exercice 2025</a></td>
                  <td class="views-field pl-0 views-field-field-company-press-releases">Legal</td>
                </tr>
              </tbody>
            </table>
          </body>
        </html>
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    fr_euronext_company_news_archive = root / "fr-euronext-paris-company-news-archive.html"
    fr_euronext_company_news_archive.write_text(
        """
        <html>
          <body>
            <table>
              <tbody>
                <tr>
                  <td class="rawmap views-field pl-0 views-field-field-company-pr-pub-datetime"><span class='nowrap'> 19 Mar 2026</span> <br><span class='nowrap'> 21:29 CET</span></td>
                  <td class="views-field pl-0 views-field-field-company-name">2CRSI</td>
                  <td class="views-field pl-0 views-field-title"><a href="" class="standardRightCompanyPressRelease" data-node-nid="12871481" data-toggle="modal" data-target="#standardRightCompanyPressRelease">Publication of the 2025 universal registration document</a></td>
                  <td class="views-field pl-0 views-field-field-company-press-releases">Other subject</td>
                </tr>
                <tr>
                  <td class="rawmap views-field pl-0 views-field-field-company-pr-pub-datetime"><span class='nowrap'> 18 Mar 2026</span> <br><span class='nowrap'> 08:15 CET</span></td>
                  <td class="views-field pl-0 views-field-field-company-name">ACTIVIUM GROUP</td>
                  <td class="views-field pl-0 views-field-title"><a href="" class="standardRightCompanyPressRelease" data-node-nid="12870001" data-toggle="modal" data-target="#standardRightCompanyPressRelease">Share buyback update</a></td>
                  <td class="views-field pl-0 views-field-field-company-press-releases">Share history</td>
                </tr>
              </tbody>
            </table>
            <nav aria-label="Page navigation">
              <ul class="pagination js-pager__items">
                <li class="page-item active"><span class="page-link">1</span></li>
                <li class="page-item"><a href="?page=1" class="page-link">2</a></li>
                <li class="page-item"><a href="?page=2" class="page-link">3</a></li>
                <li class="page-item"><a href="?page=7" title="Go to last page" class="page-link">Last</a></li>
              </ul>
            </nav>
          </body>
        </html>
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    fr_euronext_press_release_modal = root / "fr-euronext-company-press-release-12878420.html"
    fr_euronext_press_release_modal.write_text(
        """
        <div class="container" id="field_company_press_release_isin" data-node-path="/node/12878420" data-isin="FR0013344173">
          <div class="row mb-5">
            <div class="col-md-6">
              <div class="border-bottom border-ui-grey-2">
                <h3 class="text-uppercase text-sm font-weight-medium">Issuer</h3>
                <p><span class="d-inline-block text-uppercase">ROCHE BOBOIS SA</span></p>
              </div>
            </div>
          </div>
          <div class="row mb-5">
            <div class="col">
              <p><strong>ROCHE BOBOIS SA (ISIN : FR0013344173 - Mnémonique : RBO)</strong></p>
              <p>Ce Document d'Enregistrement Universel peut être consulté sur le site internet finance de Roche Bobois, <a href="https://www.finance-roche-bobois.com/fr/" target="_blank">www.finance-roche-bobois.com</a>, espace Investisseurs, rubrique Informations financières.</p>
              <p><a href="https://www.info-financiere.fr/upload/roche-bobois-document-denregistrement-universel-2025.xhtml" target="_blank">Document d'enregistrement universel 2025</a></p>
              <p><strong>CONTACT</strong></p>
              <p><strong>Actus Finance – Anne-Pauline Petureaux</strong></p>
              <p>Relations investisseurs</p>
              <p>Tél. : 01 53 67 36 72 / <a href="mailto:apetureaux@actus.fr">apetureaux@actus.fr</a></p>
              <p><strong>Actus Finance – Serena Boni</strong></p>
              <p>Relations presse</p>
              <p>Tél. : 04 72 18 04 92 / <a href="mailto:sboni@actus.fr">sboni@actus.fr</a></p>
            </div>
          </div>
        </div>
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    fr_roche_bobois_urd = (
        root / "fr-roche-bobois-document-denregistrement-universel-2025.xhtml"
    )
    fr_roche_bobois_urd.write_text(
        """
        <html xmlns="http://www.w3.org/1999/xhtml">
          <head>
            <title>Document d'enregistrement universel 2025 - Roche Bobois</title>
          </head>
          <body>
            <h1>Roche Bobois</h1>
            <h2>Conseil d'administration</h2>
            <ul>
              <li>Veronique Langlois, Administratrice indépendante</li>
            </ul>
            <h2>Directoire</h2>
            <ul>
              <li>Paul Mercier, Directeur Général Délégué</li>
            </ul>
          </body>
        </html>
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    fr_roche_bobois_governance = root / "fr-roche-bobois-governance.html"
    fr_roche_bobois_governance.write_text(
        """
        <html>
          <head>
            <title>Gouvernance - Roche Bobois</title>
          </head>
          <body>
            <h1 class="uppercase">Gouvernance</h1>
            <p><strong class="uppercase">Conseil de surveillance</strong></p>
            <div class="g-grid">
              <div class="g-block">
                <div class="g-content">
                  <p class="a-center uppercase">Jean-Eric <strong>CHOUCHAN</strong></p>
                  <p class="a-center">Président du Conseil de Surveillance</p>
                </div>
              </div>
              <div class="g-block">
                <div class="g-content">
                  <p class="a-center uppercase">Nicolas <strong>ROCHE</strong></p>
                  <p class="a-center">Vice-Président du Conseil de Surveillance</p>
                </div>
              </div>
            </div>
            <p><strong class="uppercase">Comité exécutif</strong></p>
            <div class="g-grid">
              <div class="g-block">
                <div class="g-content">
                  <p class="a-center uppercase">Guillaume <strong>DEMULIER</strong></p>
                  <p class="a-center">CEO / Président du Directoire</p>
                </div>
              </div>
              <div class="g-block">
                <div class="g-content">
                  <p class="a-center uppercase">Stéphanie <strong>BERSON</strong></p>
                  <p class="a-center">Directrice Financière Groupe / Membre du Directoire</p>
                </div>
              </div>
            </div>
          </body>
        </html>
        """.strip()
        + "\n",
        encoding="utf-8",
    )

    de_regulator = root / "de-bafin.html"
    de_regulator.write_text(
        "<html><body>BaFin landing page</body></html>\n",
        encoding="utf-8",
    )
    de_company_registry = root / "de-unternehmensregister.html"
    de_company_registry.write_text(
        "<html><body>Unternehmensregister landing page</body></html>\n",
        encoding="utf-8",
    )
    de_boerse_frankfurt = root / "de-boerse-frankfurt.html"
    de_boerse_frankfurt.write_text(
        "<html><body>Borse Frankfurt landing page</body></html>\n",
        encoding="utf-8",
    )
    de_xetra = root / "de-xetra.html"
    de_xetra.write_text(
        "<html><body>Xetra landing page</body></html>\n",
        encoding="utf-8",
    )
    de_boerse_stuttgart = root / "de-boerse-stuttgart.html"
    de_boerse_stuttgart.write_text(
        "<html><body>Borse Stuttgart landing page</body></html>\n",
        encoding="utf-8",
    )
    de_boerse_berlin = root / "de-boerse-berlin.html"
    de_boerse_berlin.write_text(
        "<html><body>Borse Berlin landing page</body></html>\n",
        encoding="utf-8",
    )
    de_boerse_hamburg = root / "de-boerse-hamburg.html"
    de_boerse_hamburg.write_text(
        "<html><body>Borse Hamburg landing page</body></html>\n",
        encoding="utf-8",
    )
    de_boerse_hannover = root / "de-boerse-hannover.html"
    de_boerse_hannover.write_text(
        "<html><body>Borse Hannover landing page</body></html>\n",
        encoding="utf-8",
    )
    de_boerse_muenchen = root / "de-boerse-muenchen.html"
    de_boerse_muenchen.write_text(
        "<html><body>Borse Munchen landing page</body></html>\n",
        encoding="utf-8",
    )
    de_boerse_duesseldorf = root / "de-boerse-duesseldorf.html"
    de_boerse_duesseldorf.write_text(
        "<html><body>Borse Dusseldorf landing page</body></html>\n",
        encoding="utf-8",
    )
    de_tradegate = root / "de-tradegate.html"
    de_tradegate.write_text(
        "<html><body>Tradegate Exchange landing page</body></html>\n",
        encoding="utf-8",
    )
    de_listed_companies = root / "de-deutsche-boerse-listed-companies.xlsx"
    _write_test_xlsx_workbook(
        de_listed_companies,
        sheets={
            "Prime Standard": [
                [
                    "ISIN",
                    "Trading Symbol",
                    "Company",
                    "Sector",
                    "Subsector",
                    "Country",
                    "Instrument Exchange",
                    "Index",
                    "TecDAX",
                ],
                [
                    "DE0007100000",
                    "MBG",
                    "MERCEDES-BENZ GROUP AG",
                    "Automobile",
                    "Automobiles",
                    "Germany",
                    "XETRA + FRANKFURT",
                    "DAX",
                    "",
                ],
                [
                    "DE0005810055",
                    "DB1",
                    "DEUTSCHE BOERSE AG",
                    "Financial Services",
                    "Financial Services",
                    "Germany",
                    "XETRA + FRANKFURT",
                    "DAX",
                    "",
                ],
                [
                    "NL0000238145",
                    "APM",
                    "AD PEPPER MEDIA EO 0,05",
                    "Media",
                    "Advertising",
                    "Netherlands",
                    "XETRA + FRANKFURT",
                    "-",
                    "",
                ],
            ],
            "Scale": [
                [
                    "ISIN",
                    "Trading Symbol",
                    "Company",
                    "Sector",
                    "Subsector",
                    "Country",
                    "Instrument Exchange",
                    "Index",
                ],
                [
                    "DE000A0M93V6",
                    "ABX",
                    "ADVANCED BLOCKCHAIN AG",
                    "Software",
                    "Software",
                    "Germany",
                    "XETRA + FRANKFURT",
                    "-",
                ],
            ],
            "Basic Board": [
                [
                    "ISIN",
                    "Trading Symbol",
                    "Company",
                    "Sector",
                    "Subsector",
                    "Country",
                    "Instrument Exchange",
                    "Index",
                ],
                [
                    "DE0005214506",
                    "ADC",
                    "ADCAPITAL AG",
                    "Automobile",
                    "Auto Parts & Equipment",
                    "Germany",
                    "FRANKFURT",
                    "-",
                ],
            ],
        },
    )
    de_maccess_listed_companies = root / "de-boerse-muenchen-maccess-listed-companies.html"
    de_maccess_listed_companies.write_text(
        """
        <html>
          <body>
            <h1>Gelistete Unternehmen in m:access</h1>
            <table>
              <thead>
                <tr>
                  <th>Emittent</th>
                  <th>WKN</th>
                  <th>Branche</th>
                  <th>m:access seit</th>
                  <th>Analystenkonferenz</th>
                  <th>Folgepflichten</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td>
                    <a target="_blank" href="https://www.aboenergy.com/de/index.php">
                      ABO Energy GmbH &amp; Co. KGaA
                    </a>
                  </td>
                  <td>
                    <a href="/aktie/DE0005760029/" target="_blank">576002</a>
                  </td>
                  <td>Technologie</td>
                  <td>26.03.2020</td>
                  <td>08.07.2026</td>
                  <td class="col-functions">
                    <a title="Jahresabschluss" target="_blank" href="https://www.aboenergy.com/de/unternehmen/geschaeftsberichte.html">JA</a>
                  </td>
                </tr>
                <tr>
                  <td>
                    <a target="_blank" href="https://advancedblockchain.com/">
                      Advanced Blockchain AG
                    </a>
                  </td>
                  <td>
                    <a href="/aktie/DE000A0M93V6/" target="_blank">A0M93V</a>
                  </td>
                  <td>Technologie</td>
                  <td>01.01.2024</td>
                  <td>15.10.2026</td>
                  <td class="col-functions">
                    <a title="Jahresabschluss" target="_blank" href="https://advancedblockchain.com/investor-relations/">JA</a>
                  </td>
                </tr>
              </tbody>
            </table>
          </body>
        </html>
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    de_duesseldorf_primary_market = root / "de-boerse-duesseldorf-primary-market.html"
    de_duesseldorf_primary_market.write_text(
        """
        <html>
          <body>
            <table class="table table-boeag table-sm">
              <thead>
                <tr>
                  <th scope="col" class="th-name">Name <span class="sub-info">ISIN</span></th>
                  <th scope="col">Kurs <span class="sub-info">Stand</span></th>
                  <th scope="col"><span class="nowrap">Diff %</span> <span class="sub-info nowrap">Diff +/-</span></th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td class="td-name">
                    <a href="/aktien/DE000A2P4HL9/123fahrschule-se-inhaber-aktien-o-n/">
                      123fahrschule SE <span class="sub-info">DE000A2P4HL9</span>
                    </a>
                  </td>
                  <td>2,540 <span class="sub-info">09:10 Uhr</span></td>
                  <td class="text-success"><span class="nowrap">0%</span><span class="sub-info nowrap">0</span></td>
                </tr>
                <tr>
                  <td class="td-name">
                    <a href="/aktien/DE000A0M93V6/advanced-blockchain-ag-inhaber-aktien-o-n/">
                      Advanced Blockchain AG <span class="sub-info">DE000A0M93V6</span>
                    </a>
                  </td>
                  <td>1,826 <span class="sub-info">09:10 Uhr</span></td>
                  <td class="text-danger"><span class="nowrap">-1,0%</span><span class="sub-info nowrap">-0,018</span></td>
                </tr>
              </tbody>
            </table>
          </body>
        </html>
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    de_supervisory_board = root / "de-mercedes-supervisory-board.html"
    de_supervisory_board.write_text(
        """
        <html>
          <head><title>Supervisory Board | Mercedes-Benz Group AG</title></head>
          <body>
            <h1>Supervisory Board</h1>
            <table>
              <tr><th>Martin Brudermuller</th><td>Chairman</td></tr>
              <tr><th>Sabine Kohleisen</th><td>Member</td></tr>
            </table>
          </body>
        </html>
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    de_board_of_management = root / "de-mercedes-board-of-management.html"
    de_board_of_management.write_text(
        """
        <html>
          <head><title>Board of Management of Mercedes-Benz Group AG</title></head>
          <body>
            <h1>Board of Management</h1>
            <h2>Ola Kallenius</h2>
            <p>Chairman of the Board of Management</p>
            <h2>Harald Wilhelm</h2>
            <p>Chief Financial Officer</p>
          </body>
        </html>
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    de_company_details = root / "de-mercedes-company-details.html"
    de_company_details.write_text(
        """
        <html>
          <head><title>Mercedes Benz Group AG (former Daimler)</title></head>
          <body>
            <h1 class="instrument-name">Mercedes Benz Group AG (former Daimler)</h1>
            <app-widget-corporate-information>
              <div class="widget widget-corporate-information ar-p h-100">
                <h2 class="widget-table-headline">Corporate information Mercedes Benz Group AG (former Daimler)</h2>
                <div class="table-responsive">
                  <table class="table widget-table">
                    <tbody>
                      <tr>
                        <td class="widget-table-cell text-nowrap">Established</td>
                        <td class="widget-table-cell">1998</td>
                      </tr>
                      <tr>
                        <td class="widget-table-cell text-nowrap">Executive board</td>
                        <td class="widget-table-cell">
                          Ola Kallenius (Chairman), Harald Wilhelm (Finance &amp; Controlling)
                        </td>
                      </tr>
                      <tr>
                        <td class="widget-table-cell text-nowrap">Supervisory board</td>
                        <td class="widget-table-cell">
                          Dr. Martin Brudermuller (Chairman), Sabine Kohleisen*<br><br>* Representative of the employees
                        </td>
                      </tr>
                      <tr>
                        <td class="widget-table-cell text-nowrap">Further information</td>
                        <td class="widget-table-cell">Company name until 31/01/2022: Daimler AG</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </app-widget-corporate-information>
            <app-widget-contact-information>
              <div class="widget app-loading-spinner-parent ar-p h-100">
                <h2 class="widget-table-headline">Contact Mercedes Benz Group AG </h2>
                <div class="table-responsive">
                  <table class="table widget-table">
                    <tbody>
                      <tr>
                        <td class="widget-table-cell text-nowrap">Phone</td>
                        <td class="widget-table-cell">+49 711 17-0</td>
                      </tr>
                      <tr>
                        <td class="widget-table-cell text-nowrap">Web</td>
                        <td class="widget-table-cell">https://group.mercedes-benz.com/</td>
                      </tr>
                      <tr>
                        <td class="widget-table-cell text-nowrap">Contact</td>
                        <td class="widget-table-cell">ir@mercedes-benz.com</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </app-widget-contact-information>
            <app-widget-shareholder>
              <div class="widget app-loading-spinner-parent ar-p h-100">
                <h2 class="widget-table-headline">Shareholder structure Mercedes Benz Group AG *</h2>
                <div class="table-responsive">
                  <table class="table widget-table">
                    <thead>
                      <tr>
                        <th class="widget-table-header-cell">Shareholder</th>
                        <th class="widget-table-header-cell">Current shares in %</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr>
                        <td class="widget-table-cell">Geely Sweden Holdings AB</td>
                        <td class="widget-table-cell">9.69</td>
                      </tr>
                      <tr>
                        <td class="widget-table-cell">Kuwait Investment Authority</td>
                        <td class="widget-table-cell">6.84</td>
                      </tr>
                      <tr>
                        <td class="widget-table-cell">Mercedes Benz Group AG (Treasury Shares)</td>
                        <td class="widget-table-cell">1.23</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </app-widget-shareholder>
          </body>
        </html>
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    de_tradegate_abo_energy = root / "de-tradegate-abo-energy-order-book.html"
    de_tradegate_abo_energy.write_text(
        """
        <html>
          <body>
            <table>
              <tr>
                <th>WKN</th>
                <th>Code</th>
                <th>ISIN</th>
                <th>Trading Currency</th>
              </tr>
              <tr>
                <td>576002</td>
                <td>AB9</td>
                <td>DE0005760029</td>
                <td>EUR</td>
              </tr>
            </table>
            <div class="block"><span>ABO Energy GmbH &amp; Co. KGaA</span></div>
          </body>
        </html>
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    de_company_registry_search = root / "de-unternehmensregister-search-results.html"
    de_company_registry_search.write_text(
        """
        <html>
          <head><title>Search in Register Information</title></head>
          <body>
            <table>
              <thead>
                <tr>
                  <th>Company name</th>
                  <th>Court</th>
                  <th>Register number</th>
                  <th>Register type</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td>
                    <a href="/en/register-information/company/mercedes-benz-group-ag?court=Stuttgart&number=19360&type=HRB">
                      Mercedes-Benz Group AG
                    </a>
                  </td>
                  <td>Stuttgart</td>
                  <td>19360</td>
                  <td>HRB</td>
                </tr>
                <tr>
                  <td>
                    <a href="/en/register-information/company/mercedes-benz-services?court=Berlin&number=1000&type=HRB">
                      Mercedes-Benz Services GmbH
                    </a>
                  </td>
                  <td>Berlin</td>
                  <td>1000</td>
                  <td>HRB</td>
                </tr>
              </tbody>
            </table>
          </body>
        </html>
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    de_company_registry_search_abo_energy = (
        root / "de-unternehmensregister-search-results-abo-energy.html"
    )
    de_company_registry_search_abo_energy.write_text(
        """
        <html>
          <head><title>Search in Register Information</title></head>
          <body>
            <table>
              <thead>
                <tr>
                  <th>Company name</th>
                  <th>Court</th>
                  <th>Register number</th>
                  <th>Register type</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td>
                    <a href="/en/register-information/company/abo-energy-gmbh-co-kgaa?court=Wiesbaden&number=12024&type=HRB">
                      ABO Energy GmbH &amp; Co. KGaA
                    </a>
                  </td>
                  <td>Wiesbaden</td>
                  <td>12024</td>
                  <td>HRB</td>
                </tr>
              </tbody>
            </table>
          </body>
        </html>
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    de_company_registry_detail = root / "de-unternehmensregister-register-information.html"
    de_company_registry_detail.write_text(
        """
        <html>
          <head><title>Register information | Mercedes-Benz Group AG</title></head>
          <body>
            <h1>Mercedes-Benz Group AG</h1>
            <dl>
              <dt>Register court</dt>
              <dd>Stuttgart</dd>
              <dt>Register number</dt>
              <dd>HRB 19360</dd>
            </dl>
            <section>
              <h2>Board of Management</h2>
              <dl>
                <dt>Ola Kallenius</dt>
                <dd>Chairman of the Board of Management</dd>
                <dt>Britta Seeger</dt>
                <dd>Member of the Board of Management</dd>
              </dl>
            </section>
            <section>
              <h2>Supervisory Board</h2>
              <ul>
                <li>Martin Brudermuller, Chairman</li>
              </ul>
            </section>
          </body>
        </html>
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    de_company_registry_detail_abo_energy = (
        root / "de-unternehmensregister-register-information-abo-energy.html"
    )
    de_company_registry_detail_abo_energy.write_text(
        """
        <html>
          <head><title>Register information | ABO Energy GmbH &amp; Co. KGaA</title></head>
          <body>
            <h1>ABO Energy GmbH &amp; Co. KGaA</h1>
            <section>
              <h2>Management Board</h2>
              <dl>
                <dt>Dr. Jochen Stotmeister</dt>
                <dd>Chief Executive Officer</dd>
                <dt>Alexander Koffka</dt>
                <dd>Chief Financial Officer</dd>
              </dl>
            </section>
            <section>
              <h2>Supervisory Board</h2>
              <ul>
                <li>Dr. Andreas Höllinger, Chairman</li>
              </ul>
            </section>
          </body>
        </html>
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    de_bafin_company_database_export = root / "de-bafin-company-database.csv"
    de_bafin_company_database_export.write_text(
        "\n".join(
            [
                ";".join(
                    [
                        "NAME",
                        "BAK NR",
                        "REG NR",
                        "BAFIN-ID",
                        "LEI",
                        "NATIONALE IDENTIFIKATIONSNUMMER DER BEHÖRDE DES HERKUNFTSMITGLIEDSTAATES",
                        "PLZ",
                        "ORT",
                        "STRASSE",
                        "LAND",
                        "GATTUNG",
                        "SCHLICHTUNGSSTELLE",
                        "HANDELSNAMEN",
                        "ZWEIGNIEDERLASSUNG IN DEUTSCHLAND",
                        "KONTAKTDATEN FÜR VERBRAUCHERBESCHWERDEN",
                        "GRENZÜBERSCHREITENDE ERBRINGUNG VON KREDITDIENSTLEISTUNGEN IN",
                        "ERLAUBNISSE/ZULASSUNG/TÄTIGKEITEN",
                        "ERTEILUNGSDATUM",
                        "ENDE AM",
                        "ENDEGRUND",
                    ]
                ),
                ";".join(
                    [
                        "COMMERZBANK AG",
                        "10010010",
                        "HRB12345",
                        "50070001",
                        "851WYGNLUQLFZBSYGB56",
                        "",
                        "60311",
                        "Frankfurt am Main",
                        "Kaiserplatz 16",
                        "Deutschland",
                        "Kreditinstitut, Wertpapierinstitut",
                        "Ombudsmann der privaten Banken",
                        "Commerzbank",
                        "",
                        "beschwerde@commerzbank.example",
                        "Österreich, Frankreich",
                        "Kreditgeschäft",
                        "01.01.1970",
                        "",
                        "",
                    ]
                ),
                ";".join(
                    [
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "Depotgeschäft",
                        "01.01.1970",
                        "",
                        "",
                    ]
                ),
                ";".join(
                    [
                        "BANQUE EXEMPLE SA",
                        "20020020",
                        "RCS55555",
                        "60080002",
                        "969500EXAMPLEBANK0001",
                        "",
                        "75001",
                        "Paris",
                        "1 Rue Exemple",
                        "Frankreich",
                        "Kreditinstitut",
                        "Médiateur bancaire",
                        "Banque Exemple",
                        "",
                        "",
                        "",
                        "Einlagengeschäft",
                        "02.02.2010",
                        "",
                        "",
                    ]
                ),
                ";".join(
                    [
                        "ING BANK N.V.",
                        "30030030",
                        "KVK88888",
                        "70090003",
                        "724500A2PXFMEQ9D8H38",
                        "",
                        "60439",
                        "Frankfurt am Main",
                        "Theodor-Heuss-Allee 2",
                        "Niederlande",
                        "Kreditinstitut, Zweigniederlassung in Deutschland",
                        "Ombudsman Finanzdienstleister",
                        "ING-DiBa AG",
                        "ING Bank N.V. Frankfurt Branch",
                        "complaints@ing.example",
                        "Deutschland",
                        "Kreditgeschäft",
                        "03.03.2015",
                        "",
                        "",
                    ]
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    return {
        "ar": {
            "country_code": "ar",
            "country_name": "Argentina",
            "pack_id": "finance-ar-en",
            "description": "Argentina finance entities built from official regulator and exchange sources.",
            "sources": [
                {
                    "name": "cnv",
                    "source_url": ar_regulator.resolve().as_uri(),
                    "category": "securities_regulator",
                },
                {
                    "name": "cnv-aif",
                    "source_url": ar_aif.resolve().as_uri(),
                    "category": "filing_system",
                },
                {
                    "name": "byma",
                    "source_url": ar_byma.resolve().as_uri(),
                    "category": "exchange",
                },
                {
                    "name": "byma-listed-companies",
                    "source_url": ar_byma_listed_companies.resolve().as_uri(),
                    "category": "issuer_directory",
                },
                {
                    "name": "cnv-emitter-regimes",
                    "source_url": ar_cnv_emitter_regimes.resolve().as_uri(),
                    "category": "issuer_directory",
                },
                {
                    "name": "bymadata-all-instrument-select",
                    "source_url": ar_bymadata_all_instrument_select.resolve().as_uri(),
                    "category": "issuer_directory",
                },
                {
                    "name": "bcra-financial-institutions",
                    "source_url": ar_bcra_financial_institutions.resolve().as_uri(),
                    "category": "institution_register",
                },
            ],
            "entities": [
                {
                    "entity_type": "organization",
                    "canonical_text": "Comisión Nacional de Valores",
                    "aliases": ["CNV", "National Securities Commission"],
                    "entity_id": "finance-ar:cnv",
                    "metadata": {"country_code": "ar", "category": "securities_regulator"},
                },
                {
                    "entity_type": "organization",
                    "canonical_text": "Autopista de la Información Financiera",
                    "aliases": ["AIF"],
                    "entity_id": "finance-ar:aif",
                    "metadata": {"country_code": "ar", "category": "filing_system"},
                },
                {
                    "entity_type": "exchange",
                    "canonical_text": "Bolsas y Mercados Argentinos",
                    "aliases": ["BYMA", "Bolsas y Mercados Argentinos S.A."],
                    "entity_id": "finance-ar:byma",
                    "metadata": {"country_code": "ar", "category": "exchange"},
                },
                {
                    "entity_type": "exchange",
                    "canonical_text": "A3 Mercados",
                    "aliases": ["A3", "A3 Mercados S.A."],
                    "entity_id": "finance-ar:a3",
                    "metadata": {"country_code": "ar", "category": "exchange"},
                },
                {
                    "entity_type": "exchange",
                    "canonical_text": "Mercado Argentino de Valores",
                    "aliases": ["MAV", "Mercado Argentino de Valores S.A."],
                    "entity_id": "finance-ar:mav",
                    "metadata": {"country_code": "ar", "category": "exchange"},
                },
                {
                    "entity_type": "organization",
                    "canonical_text": "Banco Central de la República Argentina",
                    "aliases": ["BCRA", "Central Bank of Argentina"],
                    "entity_id": "finance-ar:bcra",
                    "metadata": {"country_code": "ar", "category": "central_bank"},
                },
                {
                    "entity_type": "market_index",
                    "canonical_text": "S&P Merval",
                    "aliases": ["MERVAL"],
                    "entity_id": "finance-ar:merval",
                    "metadata": {"country_code": "ar", "category": "market_index"},
                },
            ],
        },
        "us": {
            "country_code": "us",
            "country_name": "United States",
            "pack_id": "finance-us-en",
            "description": "US finance entities built from official regulator and exchange sources.",
            "sources": [
                {
                    "name": "sec",
                    "source_url": us_regulator.resolve().as_uri(),
                    "category": "securities_regulator",
                },
                {
                    "name": "sec-company-tickers",
                    "source_url": finance_raw_snapshots["sec_companies"].resolve().as_uri(),
                    "category": "issuer_directory",
                },
                {
                    "name": "sec-submissions",
                    "source_url": finance_raw_snapshots["sec_submissions"].resolve().as_uri(),
                    "category": "filing_system",
                },
                {
                    "name": "nyse",
                    "source_url": us_exchange.resolve().as_uri(),
                    "category": "exchange",
                },
            ],
            "entities": [
                {
                    "entity_type": "organization",
                    "canonical_text": "Securities and Exchange Commission",
                    "aliases": ["SEC"],
                    "entity_id": "finance-us:sec",
                    "metadata": {"country_code": "us", "category": "securities_regulator"},
                },
                {
                    "entity_type": "organization",
                    "canonical_text": "Federal Reserve",
                    "aliases": ["Fed"],
                    "entity_id": "finance-us:fed",
                    "metadata": {"country_code": "us", "category": "central_bank"},
                },
                {
                    "entity_type": "exchange",
                    "canonical_text": "New York Stock Exchange",
                    "aliases": ["NYSE"],
                    "entity_id": "finance-us:nyse",
                    "metadata": {"country_code": "us", "category": "exchange"},
                },
                {
                    "entity_type": "market_index",
                    "canonical_text": "S&P 500",
                    "aliases": ["S&P 500 Index"],
                    "entity_id": "finance-us:sp500",
                    "metadata": {"country_code": "us", "category": "market_index"},
                },
                {
                    "entity_type": "person",
                    "canonical_text": "Jane Doe",
                    "aliases": ["Jane A. Doe"],
                    "entity_id": "finance-us:jane-doe",
                    "metadata": {
                        "country_code": "us",
                        "category": "executive_officer",
                        "role_title": "Chief Executive Officer",
                    },
                },
            ],
            "people_derivation": {
                "kind": "sec_proxy",
                "companies_source": "sec-company-tickers",
                "submissions_source": "sec-submissions",
                "archive_base_url": finance_raw_snapshots["archives_root"].resolve().as_uri(),
                "output_path": "derived_sec_proxy_people_entities.json",
                "name": "derived-finance-us-sec-people",
                "adapter": "sec_proxy_plus_insider_people_derivation",
            },
        },
        "au": {
            "country_code": "au",
            "country_name": "Australia",
            "pack_id": "finance-au-en",
            "description": "Australia finance entities built from official regulator and exchange sources.",
            "sources": [
                {
                    "name": "asic",
                    "source_url": au_regulator.resolve().as_uri(),
                    "category": "securities_regulator",
                },
                {
                    "name": "asx",
                    "source_url": au_exchange.resolve().as_uri(),
                    "category": "exchange",
                },
                {
                    "name": "asx-listed-companies",
                    "source_url": au_listed_companies.resolve().as_uri(),
                    "category": "issuer_directory",
                },
                {
                    "name": "asx-board",
                    "source_url": au_board.resolve().as_uri(),
                    "category": "issuer_profile",
                },
                {
                    "name": "asx-executive-team",
                    "source_url": au_executive.resolve().as_uri(),
                    "category": "issuer_profile",
                },
            ],
            "entities": [
                {
                    "entity_type": "organization",
                    "canonical_text": "Australian Securities and Investments Commission",
                    "aliases": ["ASIC"],
                    "entity_id": "finance-au:asic",
                    "metadata": {"country_code": "au", "category": "securities_regulator"},
                },
                {
                    "entity_type": "exchange",
                    "canonical_text": "Australian Securities Exchange",
                    "aliases": ["ASX"],
                    "entity_id": "finance-au:asx",
                    "metadata": {"country_code": "au", "category": "exchange"},
                },
                {
                    "entity_type": "organization",
                    "canonical_text": "Australian Prudential Regulation Authority",
                    "aliases": ["APRA"],
                    "entity_id": "finance-au:apra",
                    "metadata": {"country_code": "au", "category": "institution_regulator"},
                },
                {
                    "entity_type": "organization",
                    "canonical_text": "Reserve Bank of Australia",
                    "aliases": ["RBA"],
                    "entity_id": "finance-au:rba",
                    "metadata": {"country_code": "au", "category": "central_bank"},
                },
                {
                    "entity_type": "market_index",
                    "canonical_text": "S&P/ASX 200",
                    "aliases": ["ASX 200"],
                    "entity_id": "finance-au:asx-200",
                    "metadata": {"country_code": "au", "category": "market_index"},
                },
            ],
        },
        "jp": {
            "country_code": "jp",
            "country_name": "Japan",
            "pack_id": "finance-jp-en",
            "description": "Japan public-market finance entities built from official regulator and exchange sources.",
            "sources": [
                {
                    "name": "fsa",
                    "source_url": jp_fsa.resolve().as_uri(),
                    "category": "securities_regulator",
                },
                {
                    "name": "edinet",
                    "source_url": jp_edinet.resolve().as_uri(),
                    "category": "filing_system",
                },
                {
                    "name": "jpx",
                    "source_url": jp_jpx.resolve().as_uri(),
                    "category": "exchange",
                },
                {
                    "name": "jpx-disclosure-gate",
                    "source_url": jp_jpx_disclosure_gate.resolve().as_uri(),
                    "category": "disclosure_portal",
                },
                {
                    "name": "jpx-english-disclosure-availability",
                    "source_url": jp_jpx_english_disclosure_availability.resolve().as_uri(),
                    "category": "issuer_directory",
                },
                {
                    "name": "jpx-listed-company-search",
                    "source_url": jp_jpx_listed_company_search.resolve().as_uri(),
                    "category": "issuer_directory",
                },
                {
                    "name": "jpx-corporate-governance-search",
                    "source_url": jp_jpx_corporate_governance_search.resolve().as_uri(),
                    "category": "issuer_profile",
                },
            ],
            "entities": [
                {
                    "entity_type": "organization",
                    "canonical_text": "Financial Services Agency",
                    "aliases": ["FSA"],
                    "entity_id": "finance-jp:fsa",
                    "metadata": {"country_code": "jp", "category": "securities_regulator"},
                },
                {
                    "entity_type": "organization",
                    "canonical_text": "EDINET",
                    "aliases": ["Electronic Disclosure for Investors' NETwork"],
                    "entity_id": "finance-jp:edinet",
                    "metadata": {"country_code": "jp", "category": "filing_system"},
                },
                {
                    "entity_type": "exchange",
                    "canonical_text": "Japan Exchange Group",
                    "aliases": ["JPX"],
                    "entity_id": "finance-jp:jpx",
                    "metadata": {"country_code": "jp", "category": "exchange_group"},
                },
                {
                    "entity_type": "exchange",
                    "canonical_text": "Tokyo Stock Exchange",
                    "aliases": ["TSE"],
                    "entity_id": "finance-jp:tse",
                    "metadata": {"country_code": "jp", "category": "exchange"},
                },
                {
                    "entity_type": "market_index",
                    "canonical_text": "Nikkei 225",
                    "aliases": [],
                    "entity_id": "finance-jp:nikkei-225",
                    "metadata": {"country_code": "jp", "category": "market_index"},
                },
            ],
        },
        "fr": {
            "country_code": "fr",
            "country_name": "France",
            "pack_id": "finance-fr-en",
            "description": "France public-market finance entities built from official regulator and exchange sources.",
            "sources": [
                {
                    "name": "amf-listed-companies-issuers",
                    "source_url": fr_amf.resolve().as_uri(),
                    "category": "securities_regulator",
                },
                {
                    "name": "euronext-paris-regulated",
                    "source_url": fr_euronext_regulated.resolve().as_uri(),
                    "category": "issuer_directory",
                },
                {
                    "name": "euronext-paris-growth",
                    "source_url": fr_euronext_growth.resolve().as_uri(),
                    "category": "issuer_directory",
                },
                {
                    "name": "euronext-paris-access",
                    "source_url": fr_euronext_access.resolve().as_uri(),
                    "category": "issuer_directory",
                },
                {
                    "name": "euronext-paris-company-news",
                    "source_url": fr_euronext_company_news.resolve().as_uri(),
                    "category": "filing_system",
                },
                {
                    "name": "euronext-paris-company-news-archive",
                    "source_url": fr_euronext_company_news_archive.resolve().as_uri(),
                    "category": "filing_system",
                },
            ],
            "entities": [
                {
                    "entity_type": "organization",
                    "canonical_text": "Autorité des marchés financiers",
                    "aliases": ["AMF", "French Financial Markets Authority"],
                    "entity_id": "finance-fr:amf",
                    "metadata": {"country_code": "fr", "category": "securities_regulator"},
                },
                {
                    "entity_type": "exchange",
                    "canonical_text": "Euronext Paris",
                    "aliases": [],
                    "entity_id": "finance-fr:euronext-paris",
                    "metadata": {"country_code": "fr", "category": "exchange"},
                },
                {
                    "entity_type": "exchange",
                    "canonical_text": "Euronext Growth Paris",
                    "aliases": ["Paris Growth Market"],
                    "entity_id": "finance-fr:euronext-growth-paris",
                    "metadata": {"country_code": "fr", "category": "exchange"},
                },
                {
                    "entity_type": "exchange",
                    "canonical_text": "Euronext Access Paris",
                    "aliases": ["Paris Access Market"],
                    "entity_id": "finance-fr:euronext-access-paris",
                    "metadata": {"country_code": "fr", "category": "exchange"},
                },
                {
                    "entity_type": "organization",
                    "canonical_text": "Banque de France",
                    "aliases": [],
                    "entity_id": "finance-fr:banque-de-france",
                    "metadata": {"country_code": "fr", "category": "central_bank"},
                },
                {
                    "entity_type": "organization",
                    "canonical_text": "Autorité de contrôle prudentiel et de résolution",
                    "aliases": ["ACPR"],
                    "entity_id": "finance-fr:acpr",
                    "metadata": {"country_code": "fr", "category": "institution_regulator"},
                },
                {
                    "entity_type": "market_index",
                    "canonical_text": "CAC 40",
                    "aliases": [],
                    "entity_id": "finance-fr:cac-40",
                    "metadata": {"country_code": "fr", "category": "market_index"},
                },
            ],
        },
        "de": {
            "country_code": "de",
            "country_name": "Germany",
            "pack_id": "finance-de-en",
            "description": "Germany finance entities built from official regulator and exchange sources.",
            "sources": [
                {
                    "name": "bafin",
                    "source_url": de_regulator.resolve().as_uri(),
                    "category": "securities_regulator",
                },
                {
                    "name": "unternehmensregister",
                    "source_url": de_company_registry.resolve().as_uri(),
                    "category": "company_registry",
                },
                {
                    "name": "boerse-frankfurt",
                    "source_url": de_boerse_frankfurt.resolve().as_uri(),
                    "category": "exchange",
                },
                {
                    "name": "xetra",
                    "source_url": de_xetra.resolve().as_uri(),
                    "category": "exchange",
                },
                {
                    "name": "boerse-stuttgart",
                    "source_url": de_boerse_stuttgart.resolve().as_uri(),
                    "category": "exchange",
                },
                {
                    "name": "boerse-berlin",
                    "source_url": de_boerse_berlin.resolve().as_uri(),
                    "category": "exchange",
                },
                {
                    "name": "boerse-hamburg",
                    "source_url": de_boerse_hamburg.resolve().as_uri(),
                    "category": "exchange",
                },
                {
                    "name": "boerse-hannover",
                    "source_url": de_boerse_hannover.resolve().as_uri(),
                    "category": "exchange",
                },
                {
                    "name": "boerse-muenchen",
                    "source_url": de_boerse_muenchen.resolve().as_uri(),
                    "category": "exchange",
                },
                {
                    "name": "boerse-duesseldorf",
                    "source_url": de_boerse_duesseldorf.resolve().as_uri(),
                    "category": "exchange",
                },
                {
                    "name": "tradegate-exchange",
                    "source_url": de_tradegate.resolve().as_uri(),
                    "category": "exchange",
                },
                {
                    "name": "bafin-company-database-export",
                    "source_url": de_bafin_company_database_export.resolve().as_uri(),
                    "category": "institution_register",
                },
                {
                    "name": "deutsche-boerse-listed-companies",
                    "source_url": de_listed_companies.resolve().as_uri(),
                    "category": "issuer_directory",
                },
                {
                    "name": "boerse-muenchen-maccess-listed-companies",
                    "source_url": de_maccess_listed_companies.resolve().as_uri(),
                    "category": "issuer_directory",
                },
                {
                    "name": "boerse-duesseldorf-primary-market",
                    "source_url": de_duesseldorf_primary_market.resolve().as_uri(),
                    "category": "issuer_directory",
                },
                {
                    "name": "mercedes-supervisory-board",
                    "source_url": de_supervisory_board.resolve().as_uri(),
                    "category": "issuer_profile",
                },
                {
                    "name": "mercedes-board-of-management",
                    "source_url": de_board_of_management.resolve().as_uri(),
                    "category": "issuer_profile",
                },
                {
                    "name": "mercedes-company-details",
                    "source_url": de_company_details.resolve().as_uri(),
                    "category": "issuer_profile",
                },
            ],
            "entities": [
                {
                    "entity_type": "organization",
                    "canonical_text": "Federal Financial Supervisory Authority",
                    "aliases": ["BaFin"],
                    "entity_id": "finance-de:bafin",
                    "metadata": {"country_code": "de", "category": "securities_regulator"},
                },
                {
                    "entity_type": "organization",
                    "canonical_text": "Unternehmensregister",
                    "aliases": ["Company Register"],
                    "entity_id": "finance-de:unternehmensregister",
                    "metadata": {"country_code": "de", "category": "company_registry"},
                },
                {
                    "entity_type": "exchange",
                    "canonical_text": "Borse Frankfurt",
                    "aliases": ["Frankfurt Stock Exchange", "Borse Frankfurt"],
                    "entity_id": "finance-de:boerse-frankfurt",
                    "metadata": {"country_code": "de", "category": "exchange"},
                },
                {
                    "entity_type": "exchange",
                    "canonical_text": "Xetra",
                    "aliases": [],
                    "entity_id": "finance-de:xetra",
                    "metadata": {"country_code": "de", "category": "exchange"},
                },
                {
                    "entity_type": "exchange",
                    "canonical_text": "Borse Stuttgart",
                    "aliases": ["Börse Stuttgart", "Stuttgart Stock Exchange"],
                    "entity_id": "finance-de:boerse-stuttgart",
                    "metadata": {"country_code": "de", "category": "exchange"},
                },
                {
                    "entity_type": "exchange",
                    "canonical_text": "Borse Berlin",
                    "aliases": ["Börse Berlin", "Berlin Stock Exchange"],
                    "entity_id": "finance-de:boerse-berlin",
                    "metadata": {"country_code": "de", "category": "exchange"},
                },
                {
                    "entity_type": "exchange",
                    "canonical_text": "Borse Hamburg",
                    "aliases": ["Börse Hamburg", "Hamburg Stock Exchange"],
                    "entity_id": "finance-de:boerse-hamburg",
                    "metadata": {"country_code": "de", "category": "exchange"},
                },
                {
                    "entity_type": "exchange",
                    "canonical_text": "Borse Hannover",
                    "aliases": [
                        "Börse Hannover",
                        "Hannover Stock Exchange",
                        "Hanover Stock Exchange",
                    ],
                    "entity_id": "finance-de:boerse-hannover",
                    "metadata": {"country_code": "de", "category": "exchange"},
                },
                {
                    "entity_type": "exchange",
                    "canonical_text": "Borse Munchen",
                    "aliases": ["Börse München", "Munich Stock Exchange"],
                    "entity_id": "finance-de:boerse-muenchen",
                    "metadata": {"country_code": "de", "category": "exchange"},
                },
                {
                    "entity_type": "exchange",
                    "canonical_text": "Borse Dusseldorf",
                    "aliases": ["Börse Düsseldorf", "Dusseldorf Stock Exchange"],
                    "entity_id": "finance-de:boerse-duesseldorf",
                    "metadata": {"country_code": "de", "category": "exchange"},
                },
                {
                    "entity_type": "exchange",
                    "canonical_text": "Tradegate Exchange",
                    "aliases": ["Tradegate"],
                    "entity_id": "finance-de:tradegate-exchange",
                    "metadata": {"country_code": "de", "category": "exchange"},
                },
                {
                    "entity_type": "market_index",
                    "canonical_text": "DAX",
                    "aliases": ["DAX Index"],
                    "entity_id": "finance-de:dax",
                    "metadata": {"country_code": "de", "category": "market_index"},
                },
            ],
        },
        "uk": {
            "country_code": "uk",
            "country_name": "United Kingdom",
            "pack_id": "finance-uk-en",
            "description": "UK finance entities built from official regulator and exchange sources.",
            "sources": [
                {
                    "name": "fca",
                    "source_url": uk_regulator.resolve().as_uri(),
                    "category": "securities_regulator",
                },
                {
                    "name": "lse",
                    "source_url": uk_exchange.resolve().as_uri(),
                    "category": "exchange",
                },
            ],
            "entities": [
                {
                    "entity_type": "organization",
                    "canonical_text": "Financial Conduct Authority",
                    "aliases": ["FCA"],
                    "entity_id": "finance-uk:fca",
                    "metadata": {"country_code": "uk", "category": "securities_regulator"},
                },
                {
                    "entity_type": "organization",
                    "canonical_text": "Bank of England",
                    "aliases": ["BoE"],
                    "entity_id": "finance-uk:boe",
                    "metadata": {"country_code": "uk", "category": "central_bank"},
                },
                {
                    "entity_type": "exchange",
                    "canonical_text": "London Stock Exchange",
                    "aliases": ["LSE"],
                    "entity_id": "finance-uk:lse",
                    "metadata": {"country_code": "uk", "category": "exchange"},
                },
                {
                    "entity_type": "market_index",
                    "canonical_text": "FTSE 100",
                    "aliases": ["FTSE 100 Index"],
                    "entity_id": "finance-uk:ftse-100",
                    "metadata": {"country_code": "uk", "category": "market_index"},
                },
                {
                    "entity_type": "person",
                    "canonical_text": "John Roe",
                    "aliases": ["John Q. Roe"],
                    "entity_id": "finance-uk:john-roe",
                    "metadata": {
                        "country_code": "uk",
                        "category": "executive_officer",
                        "role_title": "Chief Financial Officer",
                    },
                },
            ],
        },
    }
