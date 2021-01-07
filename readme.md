#Moex data importer

<p>Project for downloading data from open MOEX api (https://iss.moex.com/iss/) and loading it to local storage.</p>
<p>The default local storage implementation is Clickhouse database (https://clickhouse.tech/)</p>

## Building the project

For building project you may use preinstalled `gradle` utility or use embedded one `./gradlew`.

Project can be built as a:
 - simple jar: `./gradlew clean build` 
 - uber jar: `./gradlew clean customUberJar`
 
## Preparing database
Obtain the Clickhouse database connection credentials. Then run the `resources/ddl/init.sql` script that will crate all necessary database objects.
 
## Running the importer

### Running trades import 
(https://iss.moex.com/iss/reference/35)

This option imports all trades data from https://iss.moex.com/iss/engines/futures/markets/forts/trades. It tries to request data for current, previous and pre-previous sessions (previous_session in (2, 1, 0)) automatically. 

`java -cp <CLASSPATH> ru.moex.importer.TradesLoader --config </path/to/config.properties>`

### Running candles import 
(https://iss.moex.com/iss/reference/155)

This option imports all candles between specified dates (`--from.date` and `--till.date`) for specified security (`--sec.id`) and interval (`--candles.interval`).

`java -cp <CLASSPATH> ru.moex.importer.CandlesLoader --config </path/to/config.properties> --sec.id brx0 --from.date 2020-01-01 --till.date 2020-12-31 --candles.interval 1m`

### Specifying the properties

App properties might be specified in:
 - config.properties file inside classpath
 - any other file with defining path to it through `--config` option
 - CLI app invocation as `--` arguments (such options will override the previous ones)