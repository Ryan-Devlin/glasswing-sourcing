from unittest.mock import patch

import pandas as pd

import ui_theme


def _frame():
    return pd.DataFrame([
        {"Domain": "visible.ai", "Select": False},
        {"Domain": "hidden.ai", "Select": False},
    ])


def test_hidden_rows_are_not_accidentally_unhidden_when_omitted():
    editor_value = pd.DataFrame([
        {"Hide": False, "Domain": "visible.ai", "Select": False},
    ])
    with (
        patch("classification_store.hidden_domain_set", return_value={"hidden.ai"}),
        patch("classification_store.hide_domains") as hide,
        patch("classification_store.unhide_domains") as unhide,
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
        {"Hide": False, "Domain": "visible.ai", "Select": False},
        {"Hide": False, "Domain": "hidden.ai", "Select": False},
    ])
    with (
        patch("classification_store.hidden_domain_set", return_value={"hidden.ai"}),
        patch("classification_store.hide_domains") as hide,
        patch("classification_store.unhide_domains") as unhide,
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
        {"Hide": True, "Domain": "visible.ai", "Select": False},
        {"Hide": False, "Domain": "hidden.ai", "Select": False},
    ])
    with (
        patch("classification_store.hidden_domain_set", return_value=set()),
        patch("classification_store.hide_domains") as hide,
        patch("classification_store.unhide_domains") as unhide,
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
