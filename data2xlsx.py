import polars as pl

columns = [
"ID","Price","Date","Postcode","PropertyType",
"NewBuild","Tenure","PAON","SAON","Street",
"Locality","Town","District","County","Category","Status"
]

df = pl.read_csv(
"pp-2025.csv",
has_header=False,
new_columns=columns,
ignore_errors=True,
infer_schema_length=0
)

df = df.with_columns([
pl.col("Price").cast(pl.Int64, strict=False),
(pl.col("NewBuild")=="Y").cast(pl.Int8).alias("is_new")
])

df = df.drop_nulls(["Price","Postcode"])

high = df.filter(
pl.col("Price") >= 250000
)

zones = (
high
.group_by("Postcode")
.agg([
pl.len().alias("transactions"),
pl.sum("Price").alias("capital"),
pl.mean("Price").alias("avg_price"),
pl.sum("is_new").alias("new_builds")
])
.with_columns(
(
pl.col("transactions")*3 +
(pl.col("capital")/1000000) +
(pl.col("new_builds")*2)
).alias("score")
)
.sort("score",descending=True)
)

zones = zones.with_columns([
pl.col("capital").map_elements(lambda x: f"${x:,.0f}"),
pl.col("avg_price").map_elements(lambda x: f"${x:,.0f}")
])

zones.write_excel("investor_zones.xlsx")

print(zones.head(25))