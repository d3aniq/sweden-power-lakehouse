# sweden-power-lakehouse

Två svenska myndigheter publicerar elproduktion per elområde. Svenska
kraftnät mäter timme för timme i kWh, SCB publicerar månadsstatistik i
GWh. De beskriver samma fysiska verklighet.

Det här projektet ställer dem mot varandra i en medallion-pipeline på
Databricks och svarar på en enkel fråga: **stämmer de överens, och när de
inte gör det, varför?**

Datan är helt publik. Ingen inloggning, inga personuppgifter, allt går att
köra om från källan.

---

## Resultatet

50 månader, januari 2021 till februari 2025, sex kraftslag och fyra
elområden.

| Kraftslag | Medianavvikelse | Riktning | Inom SCB:s avrundning |
|---|---|---|---|
| Vattenkraft | 0,07–0,19 % | Mimer något högre | 15–44 av 50 månader |
| Vindkraft | 0,09–0,17 % | Mimer något högre | 15–22 av 50 |
| Kärnkraft | 0,09 % | blandat | 13 av 50 |
| Solkraft | 39–52 % | **Mimer lägre** | 0–26 av 50 |
| Värmekraft | 37–81 % | **Mimer lägre** | 0 av 50 |

Tre kraftslag stämmer på en tiondels procent. Två gör det inte, och de
avviker åt samma håll i samtliga elområden och samtliga månader. Det är
inte brus.

### Solkraft och värmekraft: definitionsskillnad, inte fel

SCB:s egen dokumentation ger svaret på solkraften. Nätansluten solkraft
inkluderar hos SCB *uppskattad egenanvänd produktion av anläggnings-
ägaren*. Svenska kraftnät avräknar bara det som faktiskt matas in på
nätet. El som produceras på ett villatak och förbrukas i samma hus finns
alltså i den ena källan men inte i den andra, och skillnaden är ungefär
hälften av SCB:s siffra.

Värmekraften visar samma mönster men större. Den mest sannolika
förklaringen är densamma, att industriell kraftvärme som förbrukas
internt inte är med i balansavräkningen, men jag har inte belagt det i
källornas dokumentation och påstår det därför inte.

**Ingen av källorna har fel.** De svarar på olika frågor. Frågar man "hur
mycket el produceras" är SCB:s siffra rätt. Frågar man "hur mycket el når
nätet" är Svenska kraftnäts rätt. Den som jämför dem utan att veta det får
ett tal som ser ut som ett fel men inte är det.

### Augusti 2024: samma månad, två tidpunkter

Kärnkraften i SE3 ligger på 0,09 procents medianavvikelse. En enda månad
sticker ut: augusti 2024, där Svenska kraftnät ligger 322 GWh lägre,
alltså 7,5 procent.

Publiceringstidpunkten förklarar det. Varje månad 2024 publicerades
färdigt dagen efter månadsskiftet. Augusti är den enda som fick sin sista
publicering den 5 september, fyra dagar senare än alla andra. Svenska
kraftnät gick tillbaka och rättade månaden. SCB:s siffra i den hämtade
filen är preliminär och fixerad vid ett tidigare tillfälle.

Två korrekta källor, samma månad, olika svar, för att de är fixerade vid
olika tidpunkter. Det syns bara för att publiceringstidpunkten bevarades
hela vägen från bronze.

---

## Varför medallion

**Bronze** är en trogen kopia. Allt som text, inget borttaget, inget
omtolkat. Varje fil registreras med URL, tidpunkt, storlek och SHA-256 i
ett manifest. Går något fel i silver kan det köras om utan att någon källa
behöver kontaktas igen, och det går att bevisa vilken version av datan som
användes.

**Silver** är där besluten fattas, och där de går att granska. Bred form
till lång, ISO-8859-1 till UTF-8, kWh och GWh till MWh, SN-koder till
SE-koder, decimalkomma till punkt, kategorier till en gemensam vokabulär.
Varje beslut är ett stycke kod som kan läsas och ifrågasättas.

**Gold** är aggregat någon faktiskt vill se: månadsproduktion, jämförelsen
mellan källorna, och dygnsprofil per kraftslag.

Datamängden är liten nog att Pandas hade räckt. Poängen med lagren är inte
prestanda utan att varje transformation har en plats där den hör hemma,
och att det går att svara på frågan "varifrån kom den här siffran".

---

## Skillnaderna mellan källorna

Fullständig lista i [`docs/kallskillnader.md`](docs/kallskillnader.md),
skriven innan någon kod skrevs. I korthet:

| | Svenska kraftnät (Mimer) | SCB (tabell TAB78) |
|---|---|---|
| Upplösning | timme | kalendermånad |
| Enhet | kWh | GWh, heltal |
| Form | en rad per timme | en kolumn per månad |
| Områdeskod | SN1–SN4 | SE1–SE4 |
| Teckenkodning | UTF-8 med BOM | ISO-8859-1 |
| Avgränsare | semikolon | komma |
| Decimaltecken | komma | punkt |
| Status | avräknad, med publiceringstidpunkt | preliminär, revideras |

SCB-filen är formaterad för att läsas av en människa i Excel: titelrad,
tom rad, bred form, blanksteg på slutet av kategorinamnen och dubbla
mellanslag inuti dem. Fyra av de sex fel som uppstod under bygget kom från
den filen. Mimer-filerna gick rakt in.

### Noll betyder två olika saker

SCB skriver 0 för kärnkraft i SE1, SE2 och SE4. Det finns inga
kärnkraftverk där, så det är inte ett uppmätt nollvärde utan "kategorin är
inte tillämplig". Mimer svarar med en tom fil för samma kombinationer.

Silver använder Mimers manifest som facit och märker de raderna
`not_applicable` i stället för att låta dem ligga som nollor bland
uppmätta värden. Skrivs båda som 0 blir varje medelvärde över elområden
fel.

---

## Kvalitetskontroller

Jobbet failar hellre högljutt än levererar tyst fel data.

| Kontroll | Vad den fångar |
|---|---|
| Radantal mot manifest | att bronze innehåller precis det som hämtades |
| Otolkade rader | tidsstämplar och tal som inte gick att konvertera |
| Okända kategorier | en kategori som tyst skulle falla bort i joinen |
| Unikhet | dubbletter per källa, område, kraftslag och timme |
| Timmar per dygn | ofullständiga dygn, och tidszonsfrågan |
| Negativa värden | utom vattenkraft, där pumpkraft kan ge negativ netto |
| Ofullständiga månader | månader som annars summerar för lågt utan att det syns |
| Energibalans silver mot gold | att aggregeringen inte tappar något |
| **Solkraft på natten** | fel kraftslag under rätt namn |

Den sista är den viktigaste, och den enda som bygger på hur verkligheten
fungerar i stället för på datatyper. Kraftslaget står bara i Mimers
URL-parameter, inte i filen. En felaktig kod ger inte ett felmeddelande
utan **rätt namn på fel data**, vilket passerar varje formell kontroll.
Solkraft klockan tre på natten ska vara noll. Är den inte det har något
mappats fel.

Två kontroller fick skrivas om under bygget:

- Kontrollen av teckenkodning letade efter ersättningstecken. ISO-8859-1
  kan avkoda vilken bytesekvens som helst utan att klaga och producerar
  därför aldrig ett sådant tecken. Kontrollen kunde alltså aldrig falla.
  Den letar nu efter ett ord som måste finnas i filen.
- Nattkontrollen för solkraft var för sträng och slog på verklig
  produktion i gryning och skymning. Den täcker nu bara timmarna då solen
  är säkert nere.

En kvalitetskontroll som inte kan falla är ingen kvalitetskontroll.

---

## Tidszonen

Mimer anger perioder som `2024-01-01 00:00` utan tidszon. Är det svensk
lokal tid har sommartidsdygnet i mars 23 timmar och dygnet i oktober 25.
Årssumman avslöjar ingenting, eftersom de tar ut varandra.

Kontrollen av timmar per dygn ger 24 timmar för samtliga dygn, inklusive
sommartidsdygnen. Tidsstämplarna är alltså inte lokal tid med sommartid.
Utan den kontrollen hade en månadssumma blivit fel med en timme två
gånger om året.

---

## Köra själv

```bash
# 1. Hämta rådata från Mimer (endast standardbiblioteket)
python ingest/download_mimer.py

# 2. Ladda ner SCB tabell TAB78 manuellt till data/raw/scb/
```

Ladda sedan upp filerna till volymen `workspace.bronze.landing` i
Databricks Free Edition och kör notebookarna i ordning:

| Notebook | Vad den gör |
|---|---|
| `notebooks/01_bronze.py` | rådata till Delta, oförändrad |
| `notebooks/02_silver.py` | normalisering och kvalitetsgrindar |
| `notebooks/03_gold.py` | aggregat och källjämförelse |

Allt körs på Databricks Free Edition utan kostnad.

---

## Vad jag skulle göra annorlunda i skarpt läge

- **Inkrementell laddning.** Pipelinen skriver om allt vid varje körning.
  Det är rimligt för fyra år historik som inte ändras, men fel för en
  källa som uppdateras dagligen. Rätt lösning är merge på nyckel med
  publiceringstidpunkt som versionsfält.
- **Orkestrering.** Notebookarna körs manuellt i ordning. Databricks Jobs
  klarar kedjan, men beroenden mellan steg och omkörning vid fel hör
  hemma i Airflow eller Lakeflow.
- **Testtäckning.** Kvalitetskontrollerna körs på riktig data. Det finns
  inga enhetstester på transformationerna själva, med syntetiska fall för
  decimalkomma, sommartid och tomma källsvar.
- **Historik.** SCB revideras, men bara den senaste hämtningen sparas per
  datummapp. En riktig lösning behåller varje vintage och kan svara på
  "vad sa SCB om mars 2023, i mars 2023".
- **eSett.** Mimer slutar publicera 2025-03-17, därefter ligger datan hos
  eSett. Att lägga till den källan är nästa steg, och den blir ett test på
  om lagren verkligen är löst kopplade.

---

## Stack

Databricks Free Edition (serverless), PySpark, Delta Lake, Unity Catalog,
Python.

Kodkommentarerna är på svenska, eftersom källorna och deras dokumentation
är det.

## Källor

- Svenska kraftnät, Mimer: <https://mimer.svk.se/ProductionConsumption/ProductionIndex>
- SCB, tabell TAB78, Elproduktion och elanvändning efter elområde:
  <https://www.statistikdatabasen.scb.se/pxweb/sv/ssd/START__EN__EN0108__EN0108A/ElEO/>
- API: `https://api.scb.se/OV0104/v1/doris/sv/ssd/START/EN/EN0108/EN0108A/ElEO`
