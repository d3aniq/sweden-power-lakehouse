# Differences between the sources

Written before any transformation was built, based on the actual files.
Every point here corresponds to a concrete step in the silver layer.

| | Svenska kraftnät (Mimer) | SCB (table TAB78) |
|---|---|---|
| Resolution | hour | calendar month |
| Unit | kWh | GWh |
| Shape | one row per hour | one row per category and zone, one column per month |
| Zone code | SN1-SN4 | SE1-SE4 |
| Encoding | UTF-8 with BOM | ISO-8859-1 |
| Delimiter | semicolon | comma |
| Decimal mark | comma | point (no decimals in practice) |
| Status | settled, with publication timestamp | preliminary, revised |

## 1. Shape

The SCB file is wide: rows are category and bidding zone, columns are
months from 2021M01 onwards. It has to be narrowed to one row per
category, zone and month before it can be compared to anything.

Mimer is already narrow, but has one row per hour and zone and one file
per production type. The production type appears only in the file name
and the download URL, never in the content.

## 2. Encoding

The SCB file is not UTF-8. Read as UTF-8, "elområde" (bidding zone)
becomes "elomr?de". Read it as ISO-8859-1.

The Mimer file is UTF-8 with a BOM. The BOM has to go, or the first
column is named "\ufeffPeriod".

Note that `spark.read.text()` takes no encoding option and always assumes
UTF-8. The SCB file must be read through the CSV reader with a delimiter
that does not occur in the data.

## 3. Categories that do not correspond

SCB has five production categories, Mimer has six. Mimer's "uppmätt
ospecificerad produktion" (measured unspecified) has no SCB counterpart,
and SCB's "konventionell värmekraft" (conventional thermal) includes
diesel plants.

The category names in the SCB file also carry trailing spaces and double
spaces inside them, for example `"el-, gas-,  värme- och vattenverk "`.
Both trimming and whitespace collapsing are required before joining.

The mapping belongs in its own table, not in if-statements.

## 4. Zero means two different things

SCB writes 0 for nuclear in SE1, SE2 and SE4, every month. There are no
nuclear plants there, so it is not a measured zero but "this category
does not apply". Mimer delivers no file at all for that combination.

Silver must distinguish "measured as zero" from "does not exist". Writing
both as 0 loses the information, and any average across bidding zones
becomes wrong.

## 5. The definitions differ

- SCB's grid-connected solar includes estimated self-consumed production
  by the plant owner. Mimer measures what is fed into the grid. SCB's
  solar figure is therefore systematically higher.
- SCB's electricity use per bidding zone is, in SCB's own words, largely
  model-based.
- SCB is preliminary and gets revised. Mimer carries a publication
  timestamp per value, and the same hour can be republished.

This is the point of the project: the numbers are not supposed to match
exactly, and the gold layer should show how much they differ and where.

## 6. Time and time zone

Mimer states periods as "2024-01-01 00:00" without a time zone. If that
is Swedish local time, the last Sunday in March has 23 hours and the last
Sunday in October has 25. An annual total reveals nothing, since the two
cancel out — the check must count hours per day.

The check in silver returns 24 hours for every day, including the DST
days. The timestamps are therefore not local time with daylight saving.

Without this check, a monthly sum built from hourly values would be off
by one hour twice a year.

## 7. Number format

Mimer uses a decimal comma in some rows, for example
`2024-03-22 02:00;5810577,200`. Read the column as text and then as a
number with a point, and the value ends up a thousand times too large.
Note that this row falls on the night of the last Sunday in March.

## 8. Overlapping period

Mimer has production data through 2025-03-17, after which it is published
by eSett. The SCB table runs to 2026M07. The comparison can only be made
on whole months present in both, that is 2021-01 to 2025-02.

## Sources

- Mimer: `https://mimer.svk.se/ProductionConsumption/DownloadText` with the
  parameters PeriodFrom, PeriodTo, ConstraintAreaId and ProductionSortId.
- SCB: table TAB78, `https://api.scb.se/OV0104/v1/doris/sv/ssd/START/EN/EN0108/EN0108A/ElEO`
