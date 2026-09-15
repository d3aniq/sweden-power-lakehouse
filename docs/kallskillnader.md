# Skillnader mellan källorna

Skrivet innan någon transformation byggdes, utifrån de faktiska filerna.
Varje punkt här motsvarar ett konkret steg i silver-lagret.

| | Svenska kraftnät (Mimer) | SCB (tabell TAB78) |
|---|---|---|
| Upplösning | timme | kalendermånad |
| Enhet | kWh | GWh |
| Form | en rad per timme | en rad per kategori och elområde, en kolumn per månad |
| Områdeskod | SN1–SN4 | SE1–SE4 |
| Teckenkodning | UTF-8 med BOM | ISO-8859-1 |
| Avgränsare | semikolon | komma |
| Decimaltecken | komma | punkt (inga decimaler i praktiken) |
| Status | avräknad, med publiceringstidpunkt | preliminär, revideras |

## 1. Form

SCB-filen är bred: raderna är kategori och elområde, kolumnerna är månader
från 2021M01 och framåt. Den måste smalnas av till en rad per kategori,
elområde och månad innan den går att jämföra med något.

Mimer är redan smal, men har en rad per timme och elområde och en fil per
kraftslag. Kraftslaget står bara i filnamnet och i nedladdnings-URL:en,
aldrig i innehållet.

## 2. Teckenkodning

SCB-filen är inte UTF-8. Läses den som UTF-8 blir "elområde" till
"elomr?de". Läs den som ISO-8859-1 (latin-1).

Mimer-filen är UTF-8 med BOM. BOM:en måste bort, annars heter första
kolumnen "\ufeffPeriod".

## 3. Kategorier som inte motsvarar varandra

SCB har fem produktionskategorier, Mimer har sex. Mimers "uppmätt
ospecificerad produktion" har ingen motsvarighet hos SCB, och SCB:s
"konventionell värmekraft" inkluderar dieselkraftverk.

Kategorinamnen i SCB-filen har dessutom blanksteg på slutet och dubbla
mellanslag inuti, till exempel `"el-, gas-,  värme- och vattenverk "`.
Både trimning och hopslagning av inre blanksteg krävs före join.

Mappningen ska ligga i en egen tabell, inte i if-satser.

## 4. Noll betyder två olika saker

SCB skriver 0 för kärnkraft i SE1, SE2 och SE4, alla månader. Det finns
inga kärnkraftverk där, så det är inte ett uppmätt nollvärde utan
"kategorin är inte tillämplig". Mimer levererar sannolikt ingen fil alls
för den kombinationen.

Silver ska skilja på "uppmätt till noll" och "finns inte". Skrivs båda
som 0 går informationen förlorad, och ett medelvärde över elområden blir
fel.

## 5. Definitionerna skiljer sig

- SCB:s nätanslutna solkraft inkluderar uppskattad egenanvänd produktion.
  Mimer mäter det som matas in på nätet. SCB:s solsiffra ska därför vara
  systematiskt högre.
- SCB:s elanvändning per elområde är enligt SCB själva i stor
  utsträckning modellbaserad.
- SCB är preliminär och revideras. Mimer har en publiceringstidpunkt per
  värde, och samma timme kan ha publicerats om.

Det här är poängen med projektet: siffrorna ska inte stämma exakt, och
gold-lagret ska visa hur mycket de skiljer sig och var.

## 6. Tid och tidszon

Mimer anger "2024-01-01 00:00" utan tidszon. Är det svensk lokal tid har
sista söndagen i mars 23 timmar och sista söndagen i oktober 25. Räkna
timmar per dygn för de datumen och se efter. Blir det 24 överallt är
tidsstämplarna sannolikt UTC.

Kontrollen behövs för att en månadssumma ur timvärden annars blir fel med
en timme två gånger om året.

## 7. Talformat

Mimer använder decimalkomma i vissa rader, till exempel
`2024-03-22 02:00;5810577,200`. Läses kolumnen som text och sedan som tal
med punkt blir värdet tusen gånger för stort. Notera att raden ligger
natten till sista söndagen i mars.

## 8. Överlappande period

Mimer har produktionsdata till och med 2025-03-17, därefter publiceras
den av eSett. SCB-tabellen går till 2026M07. Jämförelsen kan bara göras
på hela månader som finns i båda, alltså 2021-01 till 2025-02.

## Källor

- Mimer: `https://mimer.svk.se/ProductionConsumption/DownloadText` med
  parametrarna PeriodFrom, PeriodTo, ConstraintAreaId och ProductionSortId.
- SCB: tabell TAB78, `https://api.scb.se/OV0104/v1/doris/sv/ssd/START/EN/EN0108/EN0108A/ElEO`
