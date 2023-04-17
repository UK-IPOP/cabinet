"""Our cabinet of tools.

Highlights:
    - Local MetaMap operations
    - SciSpacy NER via API
    - SNOMED tree traversal
    - UMLS CUI to SNOMED CUI
    - Common data normalization tasks

In general, we try to expose the high-level functionality of these tools
at the top level of their corresponding "drawers" (e.g. `cabinet.umls_drawer`).

This way you can import a drawer and use its functionality specifically.

For example:
```python
from cabinet import umls_drawer
umls_drawer.post_ner_single("I have a headache.")
```

If you want more granular control/exposure, check out the underscore methods 
inside the drawers although this is not recommended practice.
"""
