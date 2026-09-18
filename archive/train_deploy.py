from predictive_core import DeepCycleModel

# Initialize
model = DeepCycleModel()

# Train on your Master Dataset
# (Using the filename you confirmed works)
CSV_PATH = 'brisbane_fuel_history_clean.csv' 

try:
    model.train(CSV_PATH, city_name='brisbane')
    print("\n🎉 Deployment Successful! 'models/brisbane.pkl' is ready.")
except Exception as e:
    print(f"❌ Training Failed: {e}")