# Embeddings

By using the FastText library, we can compute vector embeddings of course descriptions. This allows us to explore the landscape of courses through the descriptions.

The embedding scripts are all located within [`/ferry/embed/`](https://github.com/coursetable/ferry/tree/master/ferry/embed).

## Workflow

1. First, we preprocess our descriptions in [`prepare_text.py`](https://github.com/coursetable/ferry/tree/master/ferry/embed/prepare_text.py). We remove description duplicates (which could otherwise throw off the embedding training process) as well as title duplicates (courses from different years often have slightly different descriptions but the same title). When removing duplicates, we keep the latest version. Next, we use various regex, NLTK, and SpaCy methods to clean the text for embedding – we remove punctuation, numbers, and stop words, and we also lemmatize our words. Runtime: less than 5 minutes.
2. We then use the FastText Python API to compute 100-dimensional word embeddings in [`train_embeddings.py`](https://github.com/coursetable/ferry/tree/master/ferry/embed/train_embeddings.py). Runtime: 2 minutes.
3. Using our computed word embeddings, we calculate embeddings for each description in [`apply_embeddings.py`](https://github.com/coursetable/ferry/tree/master/ferry/embed/apply_embeddings.py). This produces a 100-dimensional embedding vector for each description (and therefore each course). Runtime: a few seconds.
4. Using our description embeddings, we reduce the 100-dimensional embeddings to 2 dimensions with UMAP in [`umap_reduce.py`](https://github.com/coursetable/ferry/tree/master/ferry/embed/umap_reduce.py). This step makes visualization easier. This takes about half a minute to run.
5. Lastly, we visualize our UMAP dimensions with [`visualize.py`](https://github.com/coursetable/ferry/tree/master/ferry/embed/visualize.py). This produces two figures: a plot of all courses from 2009-present and a plot of all courses in Fall 2020.

## Notes

The outputs from the embedding scripts are stored under [`/data/description_embeddings`](https://github.com/coursetable/ferry-data/tree/master/description_embeddings), which is part of the ferry-data submodule. However, all outputs are currently ignored due to size (the model file is over 800 MB) and redundancy (these outputs can all be generated from existing files in a few minutes).

