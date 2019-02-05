from os import getenv
from keras.models import load_model
import keras.backend as K
import numpy as np

DAY_AS_STR = getenv('DAY_AS_STR')
UNIQUE_HASH = getenv('UNIQUE_HASH')

# @todo Do we need this accuracy function here? TBD - Try exporting the Keras model as json.


def model_accuracy(y_true, y_pred):
    std = K.std(y_true, axis=1)
    where1Class = K.cast(K.abs(std - 0.4714) < 0.01, "float32")
    where2Class = K.cast(K.abs(std - 0.2357) < 0.01, "float32")
    where3Class = K.cast(K.abs(std) < 0.01, "float32")
    thresholds = where1Class * \
        K.constant(0.5) + where2Class * K.constant(0.25) + \
        where3Class * K.constant(0.16)
    thresholds = K.expand_dims(thresholds, axis=-1)

    y_pred = K.cast(y_pred > thresholds, "int32")
    y_true = K.cast(y_true > 0, "int32")
    res = y_pred + y_true
    res = K.cast(K.equal(res, 2 * K.ones_like(res)), "int32")

    res = K.cast(K.sum(res, axis=1) > 0, "float32")
    res = K.mean(res)

    return res


class ModelManager:

    def __init__(self):
        model_file = f'/opt/models/{DAY_AS_STR}_{UNIQUE_HASH}_usi_csv_en_model.h5'

        self.model = load_model(model_file, custom_objects={
                                'kerasAcc': model_accuracy})

    def predict(self, word_vec):
        return self.model.predict(word_vec.numpy())
