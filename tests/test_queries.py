from pyspark.sql.functions import col
import sys
import os

# Nastavení environmentální proměnné na test JSON
os.environ["TEST_JSON"] = "tests/testing_data.json"
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pyspark_consumer as pc

# ----------------------------------------
# Pomocná funkce pro spouštění jedné query v test módu
# ----------------------------------------
def run_query(query_name):
    sys.argv = ["pyspark_consumer.py", "--test", f"--{query_name}"]
    df = pc.main()
    assert df is not None, f"{query_name} returned None"
    return df

# ----------------------------------------
# Testy jednotlivých query
# ----------------------------------------
def test_server_counts():
    df = run_query("server-counts")
    print() 
    df.show()
    assert set(df.columns) == {"server_name", "count"}
    assert df.filter(col("server_name").isNull()).count() == 0
    assert df.filter(col("count").isNull()).count() == 0
    assert df.count() == 26
    assert df.first()["count"] == 141
    rows = df.collect()
    assert all(rows[i]["count"] >= rows[i + 1]["count"] for i in range(len(rows)-1))

def test_edits_per_minute():
    df = run_query("edits-per-minute")
    print() 
    df.show()
    assert set(df.columns) == {"minute_start", "minute_end", "edits_per_minute"}
    assert df.filter(col("edits_per_minute") < 0).count() == 0
    rows = df.collect()
    for r in rows:
        delta = r["minute_end"] - r["minute_start"]
        assert delta.seconds == 60

def test_edits_by_type():
    df = run_query("edits-by-type")
    print() 
    df.show()
    assert set(df.columns) == {"type", "count"}
    assert df.filter(col("type").isNull()).count() == 0
    assert df.filter(col("count").isNull()).count() == 0
    valid_types = {"edit", "new", "log", "categorize"}
    assert set(r["type"] for r in df.collect()).issubset(valid_types)
    assert df.filter(col("count") < 0).count() == 0

def test_user_counts():
    df = run_query("user-counts")
    print() 
    df.show()
    assert df.filter(col("user").isNull()).count() == 0
    assert df.filter(col("activity_count") <= 0).count() == 0
    rows = df.collect()
    assert all(rows[i]["activity_count"] >= rows[i + 1]["activity_count"] for i in range(len(rows)-1))

def test_avg_edit_length_change():
    df = run_query("avg-edit-length-change")
    print() 
    df.show()
    assert df.count() == 5
    rows = df.collect()
    for r in rows:
        delta = r["window_end"] - r["window_start"]
        assert delta.seconds == 120
