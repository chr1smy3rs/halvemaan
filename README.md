# Halve Maan
Halve Maan is a data pipeline built for pull data from [GitHub's GraphQL API](https://docs.github.com/en/free-pro-team@latest/graphql) and importing it into a Mongo database.

It is built upon Python and the [Luigi pipeline framework.](https://luigi.readthedocs.io/en/stable/)

## Design Considerations
Halve Maan was built intentionally with many small tasks, each charged with loading a small domain.  This was done to allow more flexibility - it is simpler to layer data as your need it on top of the already saved items, rather than go back and rebuild the model from scratch.  We found this very helpful during the exploration phase. 

That being said, a single larger query may be more rate limit friendly (<i>but we do have error handling within the code to respond to wait limits</i>).  

## Why "Halve Maan" (<i>translation: "Half Moon"</i>)
This data pipeline named after the pipeline for beer created by the Halve Maan brewery in Bruges, Belgium. (see: [Altas Obscura](https://www.atlasobscura.com/places/halve-maan-brewery-beer-pipeline)) 