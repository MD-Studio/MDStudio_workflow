FROM mdstudio/mdstudio_docker3:0.0.3

COPY . /home/mdstudio/mdstudio_workflow

RUN chown -R mdstudio:mdstudio /home/mdstudio/mdstudio_workflow
RUN chmod -R 755 /home/mdstudio

WORKDIR /home/mdstudio/mdstudio_workflow

RUN pip install -e .

USER mdstudio

CMD ["bash", "entry_point_mdstudio_workflow.sh"]
