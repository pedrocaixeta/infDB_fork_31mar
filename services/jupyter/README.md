# Jupyter Lab Service

--

## Important notes

Notes for users:
- Only data in /app/notebooks are persisted by default, please store your notebook in this path!
        - This is because /app/notebooks is mapped to a docker volume. 
        - Other paths will be lost when the container is removed.
        - Removing the docker volume will delete all notebooks INCLUDING those in /app/notebooks!
- You can backup the notebooks on your host machine using docker's copy function:
    - `docker cp <container_id>:/app/notebooks /path/on/host`
    - The `container_id can be retrieved using `docker ps`