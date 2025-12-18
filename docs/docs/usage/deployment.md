## Start infDB
The startup script simplifies the startup process if you dont want to execute each single step as shown below separately and are happy with the default configurations and passwords:
```bash
bash infdb-startup.sh
```

**Hint:** The infDB will be run as long as you stop it manually as described below even when 
the machine is restarted.

**Hint** Ensure that Docker is running. If you use Docker Desktop, start the app.


## Stop infDB
To stop all running infDB services, execute:
```bash
docker compose stop
```

## Remove infDB
To stop all running infDB services and remove them, execute:
```bash
bash docker compose down
```

## Remove infdb-loader data
To remove the downloaded infdb-loader data, execute:
```bash
bash infdb-remove.sh