**

**# data-utils

rename to something... cabinent

all sub-folders will be suffixed with "_drawer" 

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
  - a fastapi endpoint (based on internet search) can easily serve about 200req/s which is faster than scispacy model of about 100 loops per second
  - so ok to use endpoint