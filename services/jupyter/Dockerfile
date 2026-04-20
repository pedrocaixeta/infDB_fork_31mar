FROM quay.io/jupyter/base-notebook

# Copy default notebook into the working directory
COPY notebooks/ /home/jovyan/work/

# Expose Jupyter port
# EXPOSE 8888

# Jupyter config file
# COPY jupyter_notebook_config.py /root/.jupyter/jupyter_notebook_config.py

# Register and run from the same stable interpreter.
# CMD start-notebook.py \
# 	--NotebookApp.token="${SERVICES_JUPYTER_TOKEN}" \
# 	--NotebookApp.allow_origin='*' \
# 	--NotebookApp.ip='0.0.0.0' \
# 	--NotebookApp.port=8888 \