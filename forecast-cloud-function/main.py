from google.cloud import aiplatform_v1 as ai

def forecasting_batch_prediction_job_sample(event, context):
    project = "project_id"	#replace "project_id" with your GCP project id 
    display_name = "batch_predict_weekly"
    gcs_source_uri = f"gs://{event['bucket']}/{event['name']}"
    gcs_destination_output_uri_prefix = "gs://salesforecast_output/" #replace {output_bucket_name} with output bucket
    predictions_format = "csv"
    location = "us-central1"
    api_endpoint = "us-central1-aiplatform.googleapis.com"
    model_name = "projects/project_id/locations/us-central1/models/model_id" #replace {project_id} and {model_id}

    client_options = {"api_endpoint": api_endpoint}
    # Initialize client that will be used to create and send requests.
    # This client only needs to be created once, and can be reused for multiple requests.
    client = ai.JobServiceClient(client_options=client_options)
    batch_prediction_job = {
        "display_name": display_name,
        "model": model_name,
        "input_config": {
            "instances_format": predictions_format,
            "gcs_source": {"uris": [gcs_source_uri]},
        },
        "output_config": {
            "predictions_format": predictions_format,
            "gcs_destination": {"output_uri_prefix": gcs_destination_output_uri_prefix},
        },
    }
    parent = f"projects/{project}/locations/{location}"
    response = client.create_batch_prediction_job(
        parent=parent, batch_prediction_job=batch_prediction_job
    )
    print("response:", response)
