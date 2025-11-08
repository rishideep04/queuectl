># QueueCTL - CLI based background job queue system

The tool is built in Python,designed to manage asynchronous jobs with:

1. Multiple worker process
2. Persistent PostgreSQL storage
3. Automatic retries with exponential backoff
4. DLQ to store the permanently failed jobs
5. How the jobs in DLQ were retried
6. Real-time web dashboard

This tool can :

1. enqueue job
2. initiate worker process
3. process the jobs with worker processes
4. retry the failed jobs on using dlq-retry(happens automatically on executing the command)
5. handles the dead letter queue

# How to get Started ?

# **Quick SETUP** -<br/>
# **Step 1 - Cloning the Repository {Setting up the tool!!!!}** <br/>
>    git clone https://github.com/rishideep04/queuectl.git <br/>
>    cd queuectl <br/>

# **Step 2 - Create and Activate a Virtual Environment {Preventing version conflicts or any python dependencies!!!}** <br/>
>    **Create a virtual environment** <br/>
>    python -m venv venv <br/>
> 
>    **Activating the virtual environment**<br/>
>    venv\Scripts\activate      **#{works for windows}** <br/>

# **Step 3 - Install Dependencies {I have attached a requirements.txt file in the project repo,just run the command}** <br/>
>    pip install -r requirements.txt <br/>

**Before we continue,i have to clarify the reason for using PostgreSQL, which i felt a better choice for the UseCase.** <br/>

# **Why did I use PostgreSQL ?** <br/>
>Yes, I chose PostgreSQL over the other existing db.Initially I worked with SQLITE3 but i ended up with the problem of multiple writes which lead to a deadlock and on solving it (used the file locking mechanism which SQLITE is famous for!!) i ended up with concurrency  bottlenecks. <br/>

# **The Problem I Faced** <br/>
>When i have multiple jobs and multiple worker processes,my worker process on completing a task,it is trying for the next job ,but if the job is already assigned to a worker node, this is leading to a deadlock(1 job 2 worker nodes trying to access).so I solved the deadlock issue by preventing the shared state of worker process and locked the database so that only one worker process at a time can execute.This led to a Sequential Execution of the jobs by the worker nodes which is completely abiding the concept of asynchronous execution. <br/>
>
# **Coming back to the clarification**
with PostgreSQL i can use the SKIP LOCKED Mechanism where, if a worker node is executing a job,the other worker processes will skip that job and take the next job.this helped me control over multiple writes. <br/>


# **Step 4 - Setting up Database** <br/>
> 1. create a database (if required) <br/>
>    CREATE DATABASE queuectl; <br/>
> 2. DB configuration - <br/>
>    DB_CONFIG = { <br/>
>    "dbname": "queuectl", <br/>
>    "user": "postgres", <br/>
>    "**password**": "**Rishi@282004**", <br/>
>    "host": "localhost", <br/>
>    "port": 5432 <br/>
>    } <br/>
> 3. Initialize the Database <br/>
>    python cli.py init #initializing the database for storing the jobs,workers and configuration features <br/>

**Now we are good to go !!!!** <br/>
**the setup is ready to use**  <br/>

# Usage Examples — CLI Commands with Example Outputs
 1. **We initialize the Database** <br/>
    ![We initialize the Database](images/img1.jpg)
     





















