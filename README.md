## How to run project
- Navigate to terraform directory and deploy infrastructure with:
    ```
    terraform init
    terraform plan
    terraform apply
    ```

- In `stsparkstrmwesteurope` storage account on Azure Portal UI, in `data` container create next folders:
    - `output`
    - `checkpoint`
    - `agg-cities`

- On Azure Portal, navigate to "All resources" tab and select Azure Databricks Service
- In opened Databricks UI create new Notebook and import `.dbc` file from `notebooks/export` directory
- Create new cluster, fill empty strings with your properties in `cmd 2` and run notebook
- Check output and remove infrastructure with:
  ```
  terraform destroy
  ```
  
## Results
- Number of distinct hotels in the city.  
  ![image](notebooks/screenshots/1%20distinct%20hotels.PNG)

- Average/max/min temperature in the city.  
  ![image](notebooks/screenshots/2%20cities%20agg%20stats.PNG)
- Visualize incoming data in Databricks Notebook for 10 biggest cities (the biggest number of hotels in the city, one chart for one city):
  - X-axis: date (date of observation).
  - Y-axis: number of distinct hotels, average/max/min temperature.  
  ![image](notebooks/screenshots/3%20agg%20data%20visualized.PNG)
