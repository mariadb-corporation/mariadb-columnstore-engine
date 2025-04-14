# Generating documentation for MCS cli tool
- install cmapi requirements
    ```bash
    pip install -r requirements.txt
    ```
- generate markdown
    ```bash
    typer mcs_cluster_tool/__main__.py utils docs --name mcs --output README.md
    ```
    Optionally could be generated from installed package.
    ```bash
    PYTHONPATH="/usr/share/columnstore/cmapi:/usr/share/columnstore/cmapi/deps" /usr/share/columnstore/cmapi/python/bin/python3 -m  typer /usr/share/columnstore/cmapi/mcs_cluster_tool/__main__.py utils docs --name mcs --output ~/README.md
    ```
- dependencies for gem build (RHEL example)
    ```bash
    sudo dnf install make gcc redhat-rpm-config -y
    ```
- install `md2man` (for now it's the only one tool that make convertation without any issues)
    ```bash
    sudo yum install -y ruby ruby-devel
    gem install md2man
    ```
- convert to perfect `.roff` file (`man` page)
    ```bash
    md2man-roff README.md > mcs.1
    ```
- enjoy =)