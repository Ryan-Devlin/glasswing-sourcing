from unittest.mock import patch

import pandas as pd

import ui_theme


def _frame():
    return pd.DataFrame([
        {"Domain": "visible.ai", "Select": False},
        {"Domain": "hidden.ai", "Select": False},
    ])


def _basket_frame(*domains):
    return pd.DataFrame(
        [{"domain_url": f"https://{domain}", "rating": 90} for domain in domains],
        columns=["domain_url", "rating"],
    )


def test_hidden_rows_are_not_accidentally_unhidden_when_omitted():
    editor_value = pd.DataFrame([
        {
            "Hide": False,
            "Mark Yes": False,
            "Mark No": False,
            "Domain": "visible.ai",
            "Select": False,
        },
    ])
    with (
        patch("classification_store.hidden_domain_set", return_value={"hidden.ai"}),
        patch("classification_store.hide_domains") as hide,
        patch("classification_store.unhide_domains") as unhide,
        patch("common.read_basket_data", return_value=_basket_frame()),
        patch.object(ui_theme.st, "checkbox", return_value=False),
        patch.object(ui_theme.st, "data_editor", return_value=editor_value),
    ):
        rendered = ui_theme.render_hideable_table(
            _frame(),
            domain_column="Domain",
            key="test_table",
            editable_columns=["Select"],
        )

    assert rendered["Domain"].tolist() == ["visible.ai"]
    hide.assert_not_called()
    unhide.assert_not_called()


def test_unchecking_a_displayed_hidden_row_persists_unhide():
    editor_value = pd.DataFrame([
        {"Hide": False, "Mark Yes": False, "Mark No": False, "Domain": "visible.ai", "Select": False},
        {"Hide": False, "Mark Yes": False, "Mark No": False, "Domain": "hidden.ai", "Select": False},
    ])
    with (
        patch("classification_store.hidden_domain_set", return_value={"hidden.ai"}),
        patch("classification_store.hide_domains") as hide,
        patch("classification_store.unhide_domains") as unhide,
        patch("common.read_basket_data", return_value=_basket_frame()),
        patch.object(ui_theme.st, "checkbox", return_value=True),
        patch.object(ui_theme.st, "data_editor", return_value=editor_value),
        patch.object(ui_theme.st, "rerun"),
    ):
        ui_theme.render_hideable_table(
            _frame(),
            domain_column="Domain",
            key="test_table",
        )

    hide.assert_not_called()
    unhide.assert_called_once_with({"hidden.ai"})


def test_checking_a_visible_row_persists_hide():
    editor_value = pd.DataFrame([
        {"Hide": True, "Mark Yes": False, "Mark No": False, "Domain": "visible.ai", "Select": False},
        {"Hide": False, "Mark Yes": False, "Mark No": False, "Domain": "hidden.ai", "Select": False},
    ])
    with (
        patch("classification_store.hidden_domain_set", return_value=set()),
        patch("classification_store.hide_domains") as hide,
        patch("classification_store.unhide_domains") as unhide,
        patch("common.read_basket_data", return_value=_basket_frame()),
        patch.object(ui_theme.st, "data_editor", return_value=editor_value),
        patch.object(ui_theme.st, "rerun"),
    ):
        ui_theme.render_hideable_table(
            _frame(),
            domain_column="Domain",
            key="test_table",
        )

    hide.assert_called_once_with({"visible.ai"})
    unhide.assert_not_called()


def test_checking_yes_labels_domain_without_removing_row():
    editor_value = pd.DataFrame([
        {"Hide": False, "Mark Yes": True, "Mark No": False, "Domain": "visible.ai", "Select": False},
        {"Hide": False, "Mark Yes": False, "Mark No": False, "Domain": "hidden.ai", "Select": False},
    ])
    with (
        patch("classification_store.hidden_domain_set", return_value=set()),
        patch("classification_store.hide_domains"),
        patch("classification_store.unhide_domains"),
        patch("common.read_basket_data", return_value=_basket_frame()),
        patch("common.save_basket_entries") as save,
        patch("common.remove_basket_entry") as remove,
        patch.object(ui_theme.st, "data_editor", return_value=editor_value),
        patch.object(ui_theme.st, "rerun"),
    ):
        rendered = ui_theme.render_hideable_table(
            _frame(), domain_column="Domain", key="test_table"
        )

    save.assert_called_once_with(
        [("visible.ai", 90)],
        "yes",
        source="human",
        reason="inline_yes_checkbox",
    )
    remove.assert_not_called()
    assert rendered["Domain"].tolist() == ["visible.ai", "hidden.ai"]


def test_checking_no_replaces_existing_yes_without_removing_row():
    editor_value = pd.DataFrame([
        {"Hide": False, "Mark Yes": True, "Mark No": True, "Domain": "visible.ai", "Select": False},
        {"Hide": False, "Mark Yes": False, "Mark No": False, "Domain": "hidden.ai", "Select": False},
    ])
    with (
        patch("classification_store.hidden_domain_set", return_value=set()),
        patch("classification_store.hide_domains"),
        patch("classification_store.unhide_domains"),
        patch(
            "common.read_basket_data",
            side_effect=[_basket_frame("visible.ai"), _basket_frame()],
        ),
        patch("common.save_basket_entries") as save,
        patch.object(ui_theme.st, "data_editor", return_value=editor_value),
        patch.object(ui_theme.st, "rerun"),
    ):
        rendered = ui_theme.render_hideable_table(
            _frame(), domain_column="Domain", key="test_table"
        )

    save.assert_called_once_with(
        [("visible.ai", 0)],
        "no",
        source="human",
        reason="inline_no_checkbox",
    )
    assert "visible.ai" in rendered["Domain"].tolist()


def test_unchecking_current_label_removes_it_without_hiding_row():
    editor_value = pd.DataFrame([
        {"Hide": False, "Mark Yes": False, "Mark No": False, "Domain": "visible.ai", "Select": False},
        {"Hide": False, "Mark Yes": False, "Mark No": False, "Domain": "hidden.ai", "Select": False},
    ])
    with (
        patch("classification_store.hidden_domain_set", return_value=set()),
        patch("classification_store.hide_domains"),
        patch("classification_store.unhide_domains"),
        patch(
            "common.read_basket_data",
            side_effect=[_basket_frame("visible.ai"), _basket_frame()],
        ),
        patch("common.save_basket_entries") as save,
        patch("common.remove_basket_entry") as remove,
        patch.object(ui_theme.st, "data_editor", return_value=editor_value),
        patch.object(ui_theme.st, "rerun"),
    ):
        rendered = ui_theme.render_hideable_table(
            _frame(), domain_column="Domain", key="test_table"
        )

    save.assert_not_called()
    remove.assert_called_once_with("visible.ai")
    assert "visible.ai" in rendered["Domain"].tolist()
