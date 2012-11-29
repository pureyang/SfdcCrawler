Salesforce Crawler
v 1.0_B2
Created by LeanCog

* Crawls Salesforce Knowledge Articles
* configure using Salesforce API sername, password and security token

Build and create Jar using ant
e.g.
src/ant assemble -Dlwe.home=[path to installation of LucidWorks]

To see this crawler in action please put the sfdc-crawler.jar in the "app/crawlers"
directory of your LucidWorks installation, and restart LucidWorks. The admin UI for
data sources should show new data source types "Salesforce".
