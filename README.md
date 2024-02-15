# README: Data Quality Repo Using Great Expectations on Databricks

This repository facilitates developers in implementing data quality checks using Great Expectations on Databricks. By following the provided instructions, developers can quickly set up and run validation flows for their data pipelines.

## Getting Started

To begin using the repository, follow these steps:

1. **Sample Data Generation**: Execute the notebooks in the `sample_data` folder to generate two managed tables. These tables serve as the foundation for generating expectation suites and validation flows.

2. **Expectation Suite Generation**: Run the notebooks in the `great_expectations/suite_generators` folder to generate expectation suites. Two suites will be created, one for customers and one for consumers, tailored to your specific data.

3. **Configuring Data Docs**: If you wish to host the `index.html` file at an Azure Storage Account, please refer to the official Great Expectations documentation [here](https://docs.greatexpectations.io/docs/guides/setup/configuring_data_docs/host_and_share_data_docs/).

4. **Validation Flow Execution**: After configuring the data docs, execute the notebooks in the `great_expectations/validation_flows` folder to generate validation results. This step ensures that your data meets the defined expectations.

## Folder Structure

- `sample_data`: Contains notebooks for generating sample data.
- `great_expectations/suite_generators`: Includes notebooks for generating expectation suites.
- `great_expectations/validation_flows`: Contains notebooks for executing validation flows.

## Contributing

Contributions to enhance the repository are welcome! Feel free to open issues or submit pull requests.

## License

This project is licensed under the [MIT License](LICENSE).

## Acknowledgments

Special thanks to the Great Expectations community for their valuable contributions and support.






