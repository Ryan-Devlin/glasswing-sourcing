from openpyxl import Workbook, load_workbook

from competitive_landscape import _write_workbook


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
