Salesforce Crawler
Created by LeanCog

* crawler for Knowledge Articles on Salesforce
* configure using env (test or production), username, password and security token

Build and create Jar using ant
e.g.
ant -Dlwe.home=[path to installation of LucidWorks]
ant assemble -Dlwe.home=[path to installation of LucidWorks]

To see this crawler in action please put the sfdc-crawler.jar in the "app/crawlers"
directory of your LucidWorks installation, and restart LucidWorks. The admin UI for
data sources should show new data source types "Salesforce".
