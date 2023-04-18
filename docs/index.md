**

**# data-utils

rename to something... cabinet

all sub-folders will be suffixed with "_drawer" 

TODO: switch to mkdocs, boo sphinx 

:)


metamap TOS: https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/run-locally/Ts_and_Cs.html


does metamap have a "validated/authorized" endpoint?


windows install: https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/Docs/README_win32.html


Times for metamap vs scispacy...

![img](CleanShot%202023-04-12%20at%2021.00.03@2x.png)

![img](CleanShot%202023-04-12%20at%2021.00.24@2x.png)


These are "straight up" and do not involve parallelization, in that regard:

- metmap benefits about 4x improvement from IO thread-based multiprocessing
- scispacy can utilize batch processing [link](https://spacy.io/usage/processing-pipelines) and can benefit even further from CPU (cpus go zoom) parallelization in with batch processing and limiting pipeline models
- HOWEVER
  - a fastapi endpoint (based on internet search) can easily serve about 200req/s which is faster than scispacy model of about 100it/s
  - so ok to use endpoint
  - we can further unlock this using websockets


- ner endpoint that does the SNOMED traversal
  - something like... `post_ner(text: str, terminal_tree_node: str -> cui) ?


future work will turn this into a rust/python package using pyo3 and maturin... this isn't necessary for current system calls and may benefit network calls but will mostly be required for parsing the snomed source files when we support providing your own instead of just (implying we keep current functionality as well) the id-based maps

this will also be supported by rust libraries such as mmi-parser to parse out mmi output from the rest api