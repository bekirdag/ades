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
                "name": "derived-finance-us-sec-proxy-people",
                "adapter": "sec_proxy_people_derivation",
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
