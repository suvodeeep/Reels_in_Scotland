import pandas as pd
import numpy as np
from neuralprophet import NeuralProphet
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Loading and prepareing the data
def load_data(csv_path):
    """Load and preprocess the weather data"""
    # Try different encodings
    encodings = ['utf-8', 'utf-16', 'utf-8-sig', 'latin1', 'cp1252']
    df = None
    
    for encoding in encodings:
        try:
            df = pd.read_csv(csv_path, encoding=encoding)
            print(f"Successfully loaded with {encoding} encoding")
            break
        except (UnicodeDecodeError, UnicodeError):
            continue
    
    if df is None:
        raise ValueError("Could not read CSV with any supported encoding")
    
    # Convert window_start to datetime
    df['ds'] = pd.to_datetime(df['window_start'])
    
    # Clean numeric columns
    df['avg_temperature'] = pd.to_numeric(df['avg_temperature'], errors='coerce')
    df['avg_precipitation'] = pd.to_numeric(df['avg_precipitation'], errors='coerce')
    
    # Drop any rows with missing values
    df = df.dropna(subset=['ds', 'avg_temperature', 'avg_precipitation'])
    
    # Sort by timestamp
    df = df.sort_values('ds').reset_index(drop=True)
    
    return df

def prepare_prophet_data(df):
    """Prepare data in NeuralProphet format"""
    # 'ds' (datetime) and 'y' (target) columns is needed for prophet to work

    prophet_df = pd.DataFrame({
        'ds': df['ds'],
        'y': df['avg_precipitation']
    })
    
    # Check if temperature has variation (not all same values)
    temp_variation = df['avg_temperature'].std()
    has_temp_variation = temp_variation > 0.01
    
    # Add temperature as a regressor only if it varies
    if has_temp_variation:
        prophet_df['temperature'] = df['avg_temperature']
    
    return prophet_df, has_temp_variation

def train_and_predict(df, periods=30):
    """
    Train NeuralProphet model and predict future precipitation
    periods: number of 2-minute intervals to predict (30 = 1 hour)
    """
    # Prepare the data
    prophet_df, has_temp_variation = prepare_prophet_data(df)
    
    # Adjust model complexity based on data size
    n_data = len(prophet_df)
    
    if n_data < 20:
        print("Warning: Small dataset detected. Predictions may be less reliable.")
        n_changepoints = 1
        epochs = 30
        batch_size = min(8, n_data)
        train_ratio = 0.9  # Use more data for training when we have less
    elif n_data < 50:
        n_changepoints = 2
        epochs = 50
        batch_size = 16
        train_ratio = 0.85
    else:
        n_changepoints = 5
        epochs = 100
        batch_size = 32
        train_ratio = 0.8
    
    # Initializing NeuralProphet with adaptive parameters
    model = NeuralProphet(
        growth='linear',
        n_changepoints=n_changepoints,
        changepoints_range=0.8,
        trend_reg=1,
        yearly_seasonality=False,
        weekly_seasonality=False,
        daily_seasonality=(n_data >= 50),  # Only use daily seasonality in case of enough data
        epochs=epochs,
        learning_rate=0.1,
        batch_size=batch_size
    )
    
    # Add temperature as a regressor only if it varies
    if has_temp_variation:
        model.add_future_regressor('temperature')
        print("  Using temperature as a regressor")
    else:
        print("  Temperature constant - using precipitation only")
    
    # Split data for validation
    train_size = max(int(len(prophet_df) * train_ratio), min(10, len(prophet_df)))
    train_df = prophet_df[:train_size]
    
    print(f"Training on {len(train_df)} data points...")
    if n_data < 30:
        print("  Note: Using simplified model due to limited data")
    
    # Fit the model
    metrics = model.fit(train_df, freq='2min')
    
    # Create future dataframe
    future = model.make_future_dataframe(
        train_df,
        periods=periods,
        n_historic_predictions=len(train_df)
    )
    
    # For future predictions with temperature regressor
    if has_temp_variation:
        last_temp = df['avg_temperature'].iloc[-1]
        future['temperature'] = last_temp
    
    # Make predictions
    forecast = model.predict(future)
    
    return model, forecast, prophet_df

def analyze_rain_probability(forecast, threshold=0.12):
    """
    Analyze forecast and determine rain probability
    threshold: precipitation level above which we consider it "rain"
    """
    # Get future predictions (last 30 periods = next hour)
    future_predictions = forecast.tail(30)
    
    # Extract predicted values
    predicted_precip = future_predictions['yhat1'].values
    
    # Calculate statistics
    avg_predicted_precip = np.mean(predicted_precip)
    max_predicted_precip = np.max(predicted_precip)
    trend = np.polyfit(range(len(predicted_precip)), predicted_precip, 1)[0]
    
    # Calculate rain probability based on multiple factors
    rain_score = 0
    
    # Factor 1: Average precipitation level (40%)
    if avg_predicted_precip > threshold:
        rain_score += 40
    elif avg_predicted_precip > threshold * 0.7:
        rain_score += 25
    else:
        rain_score += avg_predicted_precip / threshold * 20
    
    # Factor 2: Maximum precipitation (30%)
    if max_predicted_precip > threshold * 1.5:
        rain_score += 30
    elif max_predicted_precip > threshold:
        rain_score += 20
    else:
        rain_score += max_predicted_precip / threshold * 15
    
    # Factor 3: Upward trend (30%)
    if trend > 0.001:
        rain_score += 30
    elif trend > 0:
        rain_score += 15
    
    # Normalize score to 0-100
    rain_probability = min(100, max(0, rain_score))
    
    return {
        'rain_probability': rain_probability,
        'avg_precipitation': avg_predicted_precip,
        'max_precipitation': max_predicted_precip,
        'trend': trend,
        'predictions': predicted_precip
    }

def make_decision(rain_probability, threshold=55):
    """Make the final rain/no-rain decision"""
    if rain_probability >= threshold:
        return "Sorry no making reels today, grab an umbrella!"
    else:
        return "Make reels for the next hour, princess!!"

# Main execution
def main(csv_path='weather_data.csv'):
    print("=" * 60)
    print("RAIN PREDICTION SYSTEM WITH NEURALPROPHET")
    print("=" * 60)
    print()
    
    # Load data
    print("Loading data...")
    df = load_data(csv_path)
    print(f"Loaded {len(df)} records")
    print(f"  Date range: {df['ds'].min()} to {df['ds'].max()}")
    print()
    
    # Train and predict
    print("Training NeuralProphet model...")
    model, forecast, prophet_df = train_and_predict(df, periods=30)
    print("Model trained successfully")
    print()
    
    # Analyze predictions
    print("Analyzing predictions for next hour...")
    analysis = analyze_rain_probability(forecast)
    print()
    
    # Display results
    print("=" * 60)
    print("PREDICTION RESULTS")
    print("=" * 60)
    print(f"Rain Probability: {analysis['rain_probability']:.1f}%")
    print(f"Avg Predicted Precipitation: {analysis['avg_precipitation']:.4f} mm")
    print(f"Max Predicted Precipitation: {analysis['max_precipitation']:.4f} mm")
    print(f"Trend: {'Rising' if analysis['trend'] > 0 else 'Falling'} ({analysis['trend']:.6f})")
    print()
    
    # Make final decision
    decision = make_decision(analysis['rain_probability'])
    print("=" * 60)
    print("FINAL DECISION")
    print("=" * 60)
    print(f"\n{decision}\n")
    print("=" * 60)
    
    # Show detailed predictions
    print("\nHourly Breakdown (next 60 minutes):")
    print("-" * 60)
    for i in range(0, 30, 5):  # Show every 10 minutes
        precip = analysis['predictions'][i]
        minutes = (i + 1) * 2
        print(f"  +{minutes:2d} min: {precip:.4f} mm")
    
    return model, forecast, analysis


if __name__ == "__main__":

    model, forecast, analysis = main('C:/Users/user1/OneDrive/Desktop/Data_pipeline/query_trino/weather_export_sample2.csv')