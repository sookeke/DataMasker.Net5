#Depending on the operating system of the host machines(s) that will build or run the containers, the image specified in the FROM statement may need to be changed.
#For more information, please see https://aka.ms/containercompat

FROM microsoft/dotnet:1.1-runtime-nanoserver
#ARG source
WORKDIR /app

COPY \bin\x64\Debug\publish\ .
ENTRYPOINT ["setup.exe"]