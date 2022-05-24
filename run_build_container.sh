docker build . -t duui_build_container
docker run --rm -it -v /var/run/docker.sock:/var/run/docker.sock duui_build_container:latest
