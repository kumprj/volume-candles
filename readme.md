# Volume Based Candles as Opposed to Time Base

References this project is based on: 
* [https://tradingsim.com/blog/volume-candlesticks/](https://tradingsim.com/blog/volume-candlesticks/)
* [https://towardsdatascience.com/advanced-candlesticks-for-machine-learning-ii-volume-and-dollar-bars-6cda27e3201d](https://towardsdatascience.com/advanced-candlesticks-for-machine-learning-ii-volume-and-dollar-bars-6cda27e3201d)
* [https://towardsdatascience.com/financial-machine-learning-practitioners-have-been-using-the-wrong-candlesticks-heres-why-7a3fb85b5629](https://towardsdatascience.com/financial-machine-learning-practitioners-have-been-using-the-wrong-candlesticks-heres-why-7a3fb85b5629s)



----

The short version of these articles is the theory that Volume-Based candles create better indicators than Time-based candles when trading stocks. This theory states it is because a majority of the price movement occurs in a minority amount of the candles. 

Let's take two super simple graph axis' and showcase them. 

#### A traditional 1 Year 1 Week candlestick graph for SPY.
[SPY Candle Graph](https://i.imgur.com/aQrQr6R.png)


#### Then let's change the axis, where instead of time we use Volume. 
[Volume Candle Graph](https://i.imgur.com/owi2UYh.png)


## Diving Deeper
Let's refresh on what a candle looks like in JSON:
* {'close': [257.2, 257.21, 257.69, 257.77, 257.75], 
* 'high': [257.2, 257.21, 257.69, 257.79, 257.8], 
* 'low': [257.2, 257.21, 257.3, 257.65, 257.73], 
* 'open': [257.2, 257.21, 257.3, 257.65, 257.75], 
* 's': 'ok', 'time': [1572910200, 1572910260, 1572910440, 1572910500, 1572910560], 
* 'volume': [322, 625, 9894, 1480, 2250]}


The first question to answer is what is our parameters to create a new Volume candle? So firstly, we decided to generate Volume candles using traditional 1 minute bars. Our parameter to create a new candle is surpassing the 2 week average. So let's dive deeper - 

Starting at ETF inception, or roughly 15 years, we put the last 2 weeks of 1 minute bars into a doubly linked queue, sum all the volumes, and calculate the average. We now have this *Average* figure. Let's say its 5000. We loop through the 1 minute bars (as a JSON response), summing Volumes until we surpass the *Average*. That could be one 1-Minute Bar, or fifteen. All we know is we want to make a new candle every time Volume passes the Average.  We maintain a running *current_volume* to track that. Once that *current_volume* value surpasses the *Average*, we create a new Volume candle. We could have four 1 minute bars get us to 4500, and the fifth bar be 6500. Our new Volume candle's volume would be 11000. We don't trim overflow and are letting the machine learning model interpret as it will. We also use the first 1 Minute Bar as our "Open" and the last 1 Minute Bar we use as the "Close", keeping track of High and Low as well. 

Once we create a new Volume candle, we reset our values and continue looping. This is not a fast operation unfortunately, and time complexity became a concern. I began multithreading this process because each ETF is independent. Python has nice, easy-to-use libraries for it too.