import os
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import mean_squared_error
import mlflow
from mlflow.models import infer_signature
from dotenv import load_dotenv

load_dotenv()

os.environ['DATABRICKS_HOST'] = os.getenv("DATABRICKS_HOST")
os.environ['DATABRICKS_TOKEN'] = os.getenv("DATABRICKS_TOKEN")

def train_and_push_to_databricks(X=None, y=None):
    # 1. Point MLflow to your Databricks Workspace
    # This automatically uses the DATABRICKS_HOST and DATABRICKS_TOKEN env variables
    mlflow.set_tracking_uri("databricks")
    
    # Optional: If your workspace uses Unity Catalog, uncomment the next line
    # mlflow.set_registry_uri("databricks-uc")

    # Set the experiment path (Must exist in your Databricks workspace, e.g., in your user folder)
    # Change 'your.email@company.com' to your actual Databricks login email
    experiment_path = "/Users/t2harmon@ucsd.edu/Local_Training_Experiment"
    mlflow.set_experiment(experiment_path)

    # 2. Generate Synthetic Data
    if X == None:
        np.random.seed(42)
        X = np.random.rand(100, 1) * 10
        y = np.round(0.2 + np.random.uniform(0,1, size=(100,)))

    X_df = pd.DataFrame(X, columns=["years_experience"])
    y_df = pd.DataFrame(y, columns=["salary"])

    X_train, X_test, y_train, y_test = train_test_split(X_df, y_df, test_size=0.2, random_state=42)

    # 3. Train and Log
    print("Training model locally...")
    with mlflow.start_run(run_name="Local_to_Databricks_Run"):
        model = LogisticRegression()
        model.fit(X_train, y_train)
        
        predictions = model.predict(X_test)
        mse = mean_squared_error(y_test, predictions)
        
        mlflow.log_metric("mse", mse)
        print(f"Model trained! MSE: {mse:.2f}")
        
        # 4. Push the model to Databricks
        print("Uploading model to Databricks...")
        signature = infer_signature(X_train, predictions)
        
        # Choose a name for your model in the Databricks registry
        model_name = "model.default.local_salary_predictor" 
        
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            signature=signature,
            registered_model_name=model_name
        )
        print(f"Success! Model logged and registered as: {model_name}")

if __name__ == "__main__":
    train_and_push_to_databricks()