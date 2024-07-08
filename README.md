# Silersea Cruise Scraper

This project uses Dagster + DBT to scrape Silverseas websocket + Algolia to create a data warehouse with DuckDB.
Be warned that using this too much will get your IP blocked.

## Getting started

To install this project and its Python dependencies, run:

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

## Learning more

### Changing the code locally

When developing pipelines locally, be sure to click the **Reload definition** button in the Dagster UI after you change the code. This ensures that Dagster picks up the latest changes you made.

You can reload the code using the **Deployment** page:

<details><summary>👈 Expand to view the screenshot</summary>

<p align="center">
    <img height="500" src="https://raw.githubusercontent.com/dagster-io/dagster/master/docs/next/public/images/quickstarts/basic/more-reload-code.png" />
</p>

</details>

Or from the left nav or on each job page:

<details><summary>👈 Expand to view the screenshot</summary>

<p align="center">
    <img height="500" src="https://raw.githubusercontent.com/dagster-io/dagster/master/docs/next/public/images/quickstarts/basic/more-reload-left-nav.png" />
</p>

</details>

### Using environment variables and secrets

Environment variables, which are key-value pairs configured outside your source code, allow you to dynamically modify application behavior depending on environment.

Using environment variables, you can define various configuration options for your Dagster application and securely set up secrets. For example, instead of hard-coding database credentials - which is bad practice and cumbersome for development - you can use environment variables to supply user details. This allows you to parameterize your pipeline without modifying code or insecurely storing sensitive data.

Check out [Using environment variables and secrets](https://docs.dagster.io/guides/dagster/using-environment-variables-and-secrets) for more info and examples.

### Adding new Python dependencies

You can specify new Python dependencies in `setup.py`.

### Testing

Tests are in the `assets_dbt_python_tests` directory and you can run tests using `pytest`:

```bash
pytest dagster_tests
```



### Navigating

* infrastructure/ holds Terraform code to create azure infrastructore for scrapers and Dagster.
* dagster_etl/ holds Dagster code to run pipelines when we have picked where to store data
* dagster_etl_tests/ hold tests for dagster
* dbt_project/ holds DBT code for Dagster and whatever warehouse we pick
* scraping/ holds all the scraping code. This will need some tests
* uploader/ holds the code that uploads scraped code into storage.


README for scraper.

Notes below, of various links since original https://api.digital.silversea.com/graphql/cruisePriceAndOffers no longer works. Note however the new api2 has it, but will infer your location and does not take Currency parameters, so it only returns EUR.


Uses https://api2.digital.silversea.com/ViewerGeoLocation to figure out location and market

Once that is done, some responses will infer from that. Such as

https://api2.digital.silversea.com/v2/cruisePriceAndOffers/SN240509007?language=en
Which includes pricing information (in your geolocation)

https://api2.digital.silversea.com/cruisePaymentTerms?fareCode=PortToPortV2&cruiseCode=SN240509007&currency=EUR&language=en

https://api2.digital.silversea.com/cruisePaymentTerms?fareCode=Essential&cruiseCode=SN240509007&currency=EUR&language=en

Are information about payments where the Cruise Code is Sailing Code. 

Most promising:
A websocket that takes in countrycode and returns the prices in relevant country. See socket.py for more.


For cruise and crew information
https://www.silversea.com/page-data/find-a-cruise.html/page-data.json



USE Algolia, its open, see the SailCode and availability
https://ogg7av1jsp-dsn.algolia.net/1/indexes/*/queries

SEe code example that fetches silversea from Algolia and uses query to find specific SailCode

```
import http.client
import json

conn = http.client.HTTPSConnection("ogg7av1jsp-dsn.algolia.net")

headersList = {
 "Accept": "*/*",
 "User-Agent": "Thunder Client (https://www.thunderclient.com)",
 "X-Algolia-API-Key": "4d498c12cbd77b674c5d672621bbad43",
 "X-Algolia-Application-Id": "OGG7AV1JSP",
 "Content-Type": "application/json",
 "Referer": "https://www.silversea.com/" 
}

payload = json.dumps({"requests":[{"indexName":"prod_cruises_all_languages","params":"query=SN240509007"}]})

conn.request("POST", "/1/indexes/*/queries", payload, headersList)
response = conn.getresponse()
result = response.read()

print(result.decode("utf-8"))
```

Get EUR, USD, GPB and AUD


_____

Pulling together thoughts.

1. Use Algolia index to get every sail code.
2. Use Algolia Index to get information about ship, cabin and crew size.
3. Note that Algolia has other country prices, but only advertised price (lowest price)
3. Use Websocket to get specific Cabin price and Availability for specific promotions (essentials, Door2Door, PortToPortV2)
4. Use Websocket to get specific market prices down to person.

Note:
Only 4 guests are available. Must get Websocket for all combinations.

No Child
1 Adult
2 Adult
3 Adult
4 Adult

With Child
1 Adult 1 Child
1 Adult 2 Child
1 Adult 3 Child

2 Adult 1 Child
2 Adult 2 Child

3 Adult 1 Child


## RESEARCH!


See if this is enough to get some prices instead of checking every combination.
https://api2.digital.silversea.com/v2/cruisePriceAndOffers/SL240622012?language=en

Note that this does include data about discounted prices and original prices.