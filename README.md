# hydro-telemetry-anomaly-detection-lstm
Detecting anomaly in different water waves using LSTM (long term short memory) algorithm

### Getting Started
1. create virtual environment
    - `python3 -m venv lstm`
2. activate the virtual environment 
    - `source lstm/bin/activate`
3. install packages required from requirements.txt
    - `pip install -r requirements.txt`
4. get data from database using the credentials provided by admin by running **Query.py** file
5. clean up the exported csv file with **clean.ipynb**
6. start data training and testing 
7. drop the data into database