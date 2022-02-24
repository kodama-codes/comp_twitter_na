# Comparative analysis of Twitter networks in the context of Bitcoin

This Repository includes the complete code which was written for my Bachelor Thesis in the computer science degree
program at the [FFHS](https://www.ffhs.ch/).

## Abstract of the Thesis

The prices and thus also the trading of cryptocurrencies have increased steadily over the last several years. The
disruptive potential for high returns is enticing individuals and businesses alike to invest. At the same time, there is
a growing interest in academia about the predictive power of Twitter, especially in the financial market. The aim of the
bachelor thesis is to determine which methods are suitable for a comparative network analysis of Bitcoin-specific
Twitter networks and to what extent the result of a method is related to the Bitcoin price. Experimental results
demonstrated that the bag of nodes algorithm is suitable as a comparison method, both in terms of complexity and the
quality of the results. By means of a Granger causality test, the predictive power of the results from the comparison
method with respect to the price development of Bitcoin was proven.

## Data

The tweets which were used to create the networks can be found under the following sources:

- The Bitcoin Tweets from 2018 can be found in the
  [17.7 million bitcoin tweets](https://www.kaggle.com/jaimebadiola/177-million-bitcoin-tweets) Dataset on Kaggle.

- The self crawled bitcoin tweets from January 2022 can be found in my
  posted [2022 January Bitcoin Tweets](https://www.kaggle.com/kodamacodes/2022-january-bitcoin-tweets) Dataset on
  Kaggle.

## Installation

All necessary Python libraries can be installed with provided the requirements.txt file.

```bash
pip install -r requirements.txt
```

To compare the twitter networks the [GMatch4Py](https://github.com/Jacobe2169/GMatch4py) library is used. At this point
I want to thank Dr. Jacques Fize for implementing this awesome library. All the information to install GMatch4Py can be
found in the linked Repository. Since I had some difficulties with the installation, here is a short tutorial for the
installation under Windows.

1. Microsoft Visual C++ 14.0 or greater is required. Get it with "Microsoft C++ Build Tools":
   https://visualstudio.microsoft.com/visual-cpp-build-tools/
2. Install Munkres

```bash
pip install git+https://github.com/jfrelinger/cython-munkres-wrapper.git
```

3. Clone and install the GMatch4Py Repository to a local folder

```bash
git clone https://github.com/Jacobe2169/GMatch4py.git
cd GMatch4py
pip install .
```

4. Downgrade gensim

```bash
pip uninstall gensim
pip install gensim==3.8.3
```

5. If you are facing problems with the implementation of the DeepWalk algorithm uncomment the line:

```python
# from .embedding.deepwalk import *
```

in the gmatch4py/__init__.py file

## License
[MIT](https://choosealicense.com/licenses/mit/)




