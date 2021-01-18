### Stuck model

Sometimes the training is stuck in this situation, simply restarting lead to a correct training.

```
Epoch 1/800
5617/5617 [==============================] - 24s 3ms/step - loss: 4590.8818 - mae: 55.3445 - mse: 4590.8818 - val_loss: 4590.1050 - val_mae: 55.3452 - val_mse: 4590.1050
Epoch 2/800
5617/5617 [==============================] - 25s 3ms/step - loss: 4588.8781 - mae: 55.3247 - mse: 4588.8781 - val_loss: 4590.1050 - val_mae: 55.3452 - val_mse: 4590.1050
Epoch 3/800
5617/5617 [==============================] - 25s 3ms/step - loss: 4592.9306 - mae: 55.3447 - mse: 4592.9306 - val_loss: 4590.1050 - val_mae: 55.3452 - val_mse: 4590.1050
Epoch 4/800
```

It may be a problem of Relu activation function having the first derivative equal to zero for negative values.

Try leaky ReLu?

