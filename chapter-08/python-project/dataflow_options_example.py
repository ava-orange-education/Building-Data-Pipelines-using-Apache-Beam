from apache_beam.options.pipeline_options import (
    PipelineOptions,
    GoogleCloudOptions,
    StandardOptions,
    WorkerOptions,
)


def build_options():
    options = PipelineOptions()

    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = "my-gcp-project"
    google_cloud_options.region = "us-central1"
    google_cloud_options.job_name = "beam-performance-example"
    google_cloud_options.staging_location = "gs://my-bucket/staging"
    google_cloud_options.temp_location = "gs://my-bucket/temp"

    options.view_as(StandardOptions).runner = "DataflowRunner"

    worker_options = options.view_as(WorkerOptions)
    worker_options.machine_type = "n2-standard-4"
    worker_options.num_workers = 5
    worker_options.max_num_workers = 20
    worker_options.disk_size_gb = 50

    google_cloud_options.enable_streaming_engine = True

    return options


if __name__ == "__main__":
    opts = build_options()
    print(opts.get_all_options())
