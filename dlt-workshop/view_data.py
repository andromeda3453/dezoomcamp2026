import dlt

def view_data():
    pipeline = dlt.attach("taxi_pipeline")
    dataset = pipeline.dataset()
    
    print("Row counts:")
    print(dataset.row_counts().df())
    
    print("\nSample of 'taxi_trips' table:")
    trips = dataset["taxi_trips"].to_ibis()
    sample = trips.limit(5).execute()
    print(sample)

if __name__ == "__main__":
    view_data()
