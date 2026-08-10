from openpyxl import Workbook, load_workbook

from competitive_landscape import DEFAULT_LANDSCAPE_TEMPLATE, _write_workbook


def test_landscape_export_preserves_template_shape_and_adds_sources(tmp_path):
    template = tmp_path / "template.xlsx"
    output = tmp_path / "output.xlsx"
    book = Workbook()
    sheet = book.active
    sheet.title = "Competitive Landscape"
    sheet.append(["Competitors"] + [""] * 24)
    sheet.append([""] + [f"Header {column}" for column in range(2, 26)])
    sheet.append([""] * 25)
    book.save(template)

    count = _write_workbook(template, output, {
        "target_company": "Target",
        "methodology": "Public-source research",
        "competitors": [{
            "name": "Example",
            "website": "https://example.com",
            "level_of_competition": "H",
            "summary": "Direct competitor.",
            "total_funding_m": 12.5,
            "sources": ["https://example.com/about"],
        }],
    })

    generated = load_workbook(output)
    landscape = generated["Competitive Landscape"]
    assert count == 1
    assert landscape["A3"].value == "Example"
    assert landscape["A3"].hyperlink.target == "https://example.com"
    assert landscape["B3"].value == "H"
    assert landscape["F3"].value == 12.5
    assert landscape.auto_filter.ref == "A2:Y3"
    assert landscape.freeze_panes == "B3"
    assert generated["Research Notes"]["B1"].value == "Target"


def test_landscape_export_flattens_nested_claude_company_objects(tmp_path):
    template = tmp_path / "template.xlsx"
    output = tmp_path / "output.xlsx"
    book = Workbook()
    sheet = book.active
    sheet.title = "Competitive Landscape"
    sheet.append(["Competitors"] + [""] * 24)
    sheet.append([""] * 25)
    sheet.append([""] * 25)
    book.save(template)

    _write_workbook(template, output, {
        "target_company": {
            "name": "NEYON AI",
            "website": "https://neyon.ai",
            "description": "AI factory operations platform",
        },
        "methodology": {
            "approach": "Mapped direct and adjacent competitors",
            "data_sources": ["Company websites", "Press releases"],
            "exclusions": "Pure hardware vendors",
        },
        "competitors": [{
            "name": {
                "name": "NEYON AI",
                "website": "https://neyon.ai",
                "description": "AI factory deployment platform",
            },
            "partners_brands": [{"name": "NVIDIA"}, {"name": "AWS"}],
        }],
    })

    landscape = load_workbook(output)["Competitive Landscape"]
    assert landscape["A3"].value == "NEYON AI"
    assert landscape["A3"].hyperlink.target == "https://neyon.ai"
    assert landscape["M3"].value == "NVIDIA, AWS"
    assert load_workbook(output)["Research Notes"]["B1"].value == "NEYON AI"
    assert "approach:" in load_workbook(output)["Research Notes"]["B3"].value


def test_talvy_template_maps_direct_comp_fields(tmp_path):
    output = tmp_path / "talvy-output.xlsx"
    _write_workbook(DEFAULT_LANDSCAPE_TEMPLATE, output, {
        "target_company": "Talvy",
        "competitors": [{
            "name": "Direct Co",
            "type_of_player": "Direct competitor",
            "vertical_broad": "Vertical",
            "approach": "Workflow-specific AI platform",
            "summary": "Direct product overlap.",
            "hq": "Boston, MA",
            "total_funding_m": 8,
            "website": "https://direct.example",
            "founded_years_in_operation": "2022 (4 years)",
            "ownership_private_public": "Private",
        }],
    })

    book = load_workbook(output)
    sheet = book["Comps"]
    assert sheet["A1"].value == "Incumbents"
    assert sheet["A3"].value == "Name"
    assert sheet["A4"].value == "Direct Co"
    assert sheet["B4"].value == "Direct competitor"
    assert sheet["D4"].value == "Workflow-specific AI platform"
    assert sheet["G4"].value == 8
    assert sheet["O4"].value == "https://direct.example"
    assert sheet["O4"].hyperlink.target == "https://direct.example"
    assert sheet["Q4"].value == "Private"
    assert sheet.freeze_panes == "A4"
