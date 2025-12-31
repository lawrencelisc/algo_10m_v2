# Project1


### Installation

---
1. Clone the repository

   - Using SSH
   ```shell script
   git clone xxx
   ``` 
   
   - Using HTTPS with Personal Access Token
   ```shell script
   git clone xxx
   ```

2. Set up the Virtual Environment

    Ubuntu 20.04 (Debian-based Linux)
    ```shell script
    cd ./algo_10m_v2
    python3.12 -m venv venv/
    source ./venv/bin/activate
    ```
   
    Windows 10
    ```shell script
    cd .\algo_10m_v2
    python -m venv .\venv\
    .\venv\Scripts\activate
    ```

3. Install the dependencies

    ```shell script
    pip install -r requirements.txt
    pip install --upgrade pip
    ```


### Deployment

---
#### Dev Environment
1. Run the application
    ```shell script
    python3.12 main.py
    ```

#### Running via Systemd
1. Move the file to Systemd's system folder.
    ```shell script
    sudo cp ./algo_10m_v2.service /etc/systemd/system/algo_10m_v2.service
    ```
2. Enable and start the service.
    ```shell script
    sudo systemctl daemon-reload
    sudo systemctl enable algo_10m_v2.service
    sudo systemctl start algo_10m_v2.service
    ```
3. Check if the application is running.
    ```shell script
    sudo systemctl status algo_10m_v2.service
    ```
# web_scrapping_2025-08-27
