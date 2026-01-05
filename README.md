# SL Metro Delay Prediction System

This is a serverless machine learning pipeline for predicting the average delay for the next half hour of all three SL metro lines (blå, grön, röd) in Stockholm. The predictions are based on the previous delays, the weekday, and the line color of the metro.

The dashboard displaying the forecasts and hindcasts can be found [here](https://serkananar.github.io/metro-delay-prediction/).

## Architecture explanation
**Data sources**:  
Historical and dynamic data are obtained from Trafiklab’s GTFS Regional and Static APIs.  
  
**Filtering and feature engineering**:  
Raw GTFS data is filtered to metro traffic only and transformed into aggregated features representing scheduled trips and current delay statistics per line.  
  
**Feature store**:  
All processed features are stored as versioned feature groups in Hopsworks.  
  
**Model training**:  
Training jobs retrieve historical data from the feature store to train delay prediction models.  
  
**Model registry**:  
Trained models are versioned and stored in the Hopsworks model registry.  
  
**Inference**:  
Batch inference retrieves the latest feature data from the feature store and the selected model from the model registry. The predictions are written back to the feature store for downstream use.  
  
**Monitoring and visualization**:  
Current and historical predictions of today are visualized through a dashboard hosted on GitHub Pages.

## Pipeline
The pipeline is scheduled to run every half hour using GitHub Actions at minutes 00 and 30, from 7 AM to 11 PM UTC.

```1_ingest_and_upload.py```: Ingests current traffic data from Trafiklab’s API, performs feature engineering, and uploads the resulting feature groups to Hopsworks.  
  
```2_training_pipeline.ipynb```: Features and labels are loaded from the two feature groups that were updated in the previous step in the pipeline, and features are paired up with corresponding delays from 30 minutes into the future. Then, the current day is added as a feature and both the day and line are encoded as single integer values that the model can interpret. Since the model is operating on time-series data, the data is manually split into training and test, on which the model is trained on. Lastly, the model and two artifacts, namely the encoders, are uploaded to Hopsworks so they can be downloaded for inference.  
  
```3_inference_pipeline.ipynb```:  Fetches the latest data points for all three metro lines and the model from Hopsworks’ feature store and model registry respectively, and performs inference for the next half hour. The forecasts, together with a corresponding hindcast from the last inference run, are then displayed by the online dashboard.


## Features
Currently, the following features are used in the pipeline.
| `line_encoded` | `day_encoded` | `delay_60` | `delay_30` | `delay_current` |
| --- | --- | --- | --- | --- |
| `int64` | `int64` | `float64` | `float64` | `float64` |


## Results

We trained two models: an XGB Regressor and an MLP Regressor. To measure model performance, we use the mean squared error (MSE) and R2 score as metrics. Also, a grid search is applied with the aim to get the most out of the models as possible. 

For the XGB Regressor, the grid search was done with different configurations of `n_estimators` $\in [100, 150, 200, 250]$ and `lr` $\in [0.01, 0.05, 0.1]$. The best performing model had the following parameters: `lr` $=0.1$ and `n_estimators` $= 100$. 

The MLP has 2 hidden layers, and the grid search was done with different configurations of `h1` $\in [32, 64, 128, 256]$, `h2` $\in [32, 64, 128, 256]$, `lr` $\in [0.01, 0.05, 0.1]$. The best performing model had the following parameters: `lr` $=0.05$, `h1` $=128$, and `h2` $=32$.

The best performing XGB Regressor achieved test MSE 118.8 and R2 score 0.895, while the best performing MLP Regressor achieved test MSE 107.6 and R2 score 0.905. These differences are quite small, and considering that the MSE calculations are using the squared error between the true delay and predicted delay in seconds, the predictions should not be that different from each other in practice. As our final model, we decided to use the MLP. 


## Limitations

The dependency on TrafikLab’s APIs is a big limitation, both for live data and historic data. The original plan was to use their historic data API (KoDa) to fetch data for the previous weeks and train the model on that, but fetching even one day of data could take more than 24 hours. Therefore, we decided to build our dataset with time, as fetching live data is near instant. However, this approach means that our dataset is relatively small for the first couple of weeks. 


## Future Work

- Add weather conditions as features in the pipeline
- Add delay prediction for commuter trains (pendeltåg)
- Predictions for line color, line number, and direction
- Explore more models

## How to Run

If you would like to see the UI, follow the link near the top of the README file. 

The requirements for running the code are the following: 

- Python 3.11 to 3.13.
- A Hopsworks account (and API key)
- A TrafikLab account and two API keys: GTFS Regional Realtime and GTFS Regional Static data. 

GTFS Regional Realtime and GTFS Regional Static have limited calls, but the initial tiers are enough to run the code. TrafikLab offers free API tier upgrades for those who would like to run the code more often. 

To run the code, simply install the requirements specified in ```requirements.txt``` and run the ordered steps in the pipeline folder. 

- ```1_ingest_and_upload.py```
- ```2_training_pipeline.ipynb```
- ```3_inference_pipeline.ipynb```

Also, before model training and inference, ensure that enough data has been uploaded to Hopsworks. 


