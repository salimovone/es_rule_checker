# es_rule_checker


## use on linux 

1. You need to change linux limits for send/recive hundred of queries per second. You can do it with this command

    ```bash
    ulimit -n 4096
    ```

2. Copy binary file to the system
    ```bash
    sudo cp path/to/your_app /usr/local/bin/
    ```

3. Create systemd service file
    ```bash
    sudo nano /etc/systemd/system/your_app.service
    ```

4. Add following code to that file
    ```ini
    [Unit]
    Description=Es Rule Checker
    After=network.target

    [Service]
    WorkingDirectory=/home/ubuntu
    ExecStart=/home/ubuntu/es_rule_checker
    Restart=always
    User=ubuntu
    Environment=RUST_LOG=info

    [Install]
    WantedBy=multi-user.target

    ```

    `User=` must be your linux username. (You can check it with `whoami` command)

5. Run the service 
    ```bash
    sudo systemctl daemon-reexec
    sudo systemctl daemon-reload
    sudo systemctl enable your_app.service
    sudo systemctl start your_app.service
    ```

    Check if running
    ```bash
    sudo systemctl status your_app.service
    ```