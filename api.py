from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pyspark.sql import SparkSession


app = FastAPI(
    title="Powered by Apache Hudi and Spark",
    version="1.0",
)

# Initialize remote Spark session
spark = SparkSession.builder.remote("sc://localhost").appName("FastAPIWithRemoteSpark").getOrCreate()

# Load employees_df and create a temporary view
table_name = "employees"
path = f"file:///Users/soumilshah/IdeaProjects/mdbt/tmp/{table_name}"
spark.read.format("hudi").load(path).createOrReplaceTempView("employees_view")


# Endpoint to get employee by emp_id
@app.get("/get_employee/{emp_id}", response_class=JSONResponse)
def get_employee_by_emp_id(emp_id: str):
    try:
        # Execute Spark SQL query to filter employees by emp_id
        result_df = spark.sql(f"SELECT * FROM employees_view WHERE emp_id = '{emp_id}'")

        # Convert Spark DataFrame to Pandas DataFrame for JSON serialization
        result_pandas_df = result_df.toPandas()

        # Convert Pandas DataFrame to JSON
        result_json = result_pandas_df.to_json(orient="records")

        return JSONResponse(content=result_json, status_code=200)


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Run the FastAPI application
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
