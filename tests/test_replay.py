"""--only-missing replay: only the elements the lake lacks, and that the diffs do not delete, are
applied; everything the lake already has is left alone."""
import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))
import updater  # noqa: E402


def test_missing_only_keeps_new_live_elements():
    by = {"n": {1: {"deleted": False}, 2: {"deleted": False}, 3: {"deleted": True}, 4: {"deleted": True}},
          "w": {10: {"deleted": False}, 11: {"deleted": False}},
          "r": {}}
    ex = {"n": {1: 5, 4: 9}, "w": {10: 7}, "r": {}}  # the lake has nodes 1 and 4 and way 10
    out = updater.Updater.missing_only(by, ex)
    # node 1, way 10: the lake has them (applied correctly by the earlier pass); node 3: absent and
    # deleted (created and deleted inside the window); node 4: present but deleted — the earlier pass
    # applied deletes of what it had, so a delete here is not the replay's business either
    assert out == {"n": {2: {"deleted": False}}, "w": {11: {"deleted": False}}, "r": {}}
