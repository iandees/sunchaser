import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "tools"))
from compare_overpass import inject_bbox  # noqa: E402


def test_inject_bbox():
    assert inject_bbox('[out:csv(::id,name;true;",")];node;out;', "1,2,3,4") == \
        '[out:csv(::id,name;true;",")][bbox:1,2,3,4];node;out;'
    assert inject_bbox("[out:json][timeout:5];node;out;", "1,2,3,4") == "[out:json][timeout:5][bbox:1,2,3,4];node;out;"
    assert inject_bbox("node;out;", "1,2,3,4") == "[bbox:1,2,3,4];node;out;"
