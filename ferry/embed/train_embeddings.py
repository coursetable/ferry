import fasttext

from ferry import config

model = fasttext.train_unsupervised(
    str(config.DATA_DIR / "description_embeddings/descriptions_corpus.txt"),
    model="skipgram",
    lr=0.05,
    dim=100,
    ws=5,
    epoch=100,
)

model.save_model(str(config.DATA_DIR / "description_embeddings/fasttext_model.bin"))
