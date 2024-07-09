`<a id="readme-top"></a>`

[Contributors][contributors-url] [Forks][forks-url] [Stargazers][stars-url] [Issues][issues-url] [MIT License][license-url] [LinkedIn][linkedin-url]

<!-- PROJECT LOGO -->

<br />
<div align="center">
  <a href="https://github.com/chasimm3/dota_project">
    <img src="images/Dota-2-Logo.png" alt="Logo" width="100" height="100">
  </a>

<h3 align="center">OpenDota Data Warehouse</h3>

<p align="center">
An all in one data warehouse utilising the API provided by OpenDota.com. 
    <br />
    <a href="https://github.com/chasimm3/dota_project"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    ·
    <a href="https://github.com/chasimm3/dota_project/issues/new?labels=bug&template=bug-report---.md">Report Bug</a>
    ·
    <a href="https://github.com/chasimm3/dota_project/issues/new?labels=enhancement&template=feature-request---.md">Request Feature</a>
  </p>
</div>

<!-- TABLE OF CONTENTS -->

<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>

<!-- ABOUT THE PROJECT -->

## About The Project

This project aims to provide an all-in-one ETL process to deliver a data warehouse for use in analytics and reporting on recent Dota matches involving professional players.

<br />
<div align="center">
  <a href="https://github.com/chasimm3/dota_project">
    <img src="images/Example.png" width="800" height="317">
  </a>
 </div>

An example of the data structure within the Matches fact table.

<!-- GETTING STARTED -->

## Getting Started

When executed, the script will run a series of GET API calls against the OpenDota API. It will stage the json data locally, then transform the data into a Kimball dimensional model.

Currently there are 3 dimensions and 1 fact table in the model, this are *Dim_Player*, *Dim_Item*, *Dim_Hero*, and *Fact_Matches*. *Dim_Item* is not currently connected to the star schema, but this is a planned addition in the future.

The code contains a config file which can be edited to adjust the output file structure, as well as the preferred output file types. The parameters are as follows:

**Staging Folder**

The name of the folder that will be created to store the raw *.json* data. The default file path is *Staging/*. It can be altered by replacing *Staging/* with your desired staging file path within config.py:-

```py
# the folder in which the staging json files will land
staging_folder = base_file_path + 'Staging/'
```

**Tables Folder**

The name of the folder that will be created to store the dimensions and fact tables. The default file path is *Tables/*. It can be altered by replacing *Tables/* with your desired staging file path within config.py:-

```py
# the folder in while the star schema model will land
tables_folder = base_file_path + 'Tables/'
```

**Output file type**

The output file type of the dimensions and fact tables, currently the options are *xlxs*, *parquet* (using gzip compression) and *csv*.
To change the output file type, replace *xlsx* with one of the other options in the following code within config.py:

```py
# the desired output file type of the star schema
output_file_type = 'xlsx'
```

**Output into single excel file**

If the output file type is set to *xlsx* this option determines whether the tables will be output to sheets within a single workbook, or seperate workbooks by table. To change the output type, replace *True* with *False* in the following code within config.py:

```py
# if xlsx if the desired output type, this will determine whether the tables are loaded
# to individual files or whether they will be loaded as sheets into the same file
output_file_excel_single_file = True
```

### Prerequisites

A basic knowledge of running python code, optionally including changing parameters before execution. This is best done in an IDE such as VSCode.

Ensure that you have pip installed and are upgraded to the latest version.

##### Windows:

```py
  py -m ensurepip --upgrade
```

##### Linux:

```py
  python -m ensurepip --upgrade
```

##### MacOS

```py
  python -m ensurepip --upgrade
```

### Installation

1. Clone the main branch from the repo into your desired IDE.

```sh
   git clone https://github.com/chasimm3/dota_project.git
```

2. Open config.py and make any desired changes to the optional parameters.
3. Save and close config.py
4. Execute the main.py file.
5. Once complete, the files will be available in the structure specified in config.py.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- USAGE EXAMPLES -->

## Usage

The primary use of this project is for data analysis of trends of professional Dota players. The data can be connected to any data analysis tool (e.g. PowerBi, Jupyter etc) to enable in-depth analysis of the hero choice upon win probability.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## API Usage Limits

As the code utilises the free API provided by OpenDota.com, it is as such limited to:

- 60 requests per min
- 2,000 requests per day

There are currently no plans to include a premium option, if you would like this functionality added feel free to raise a feature-request at: https://github.com/chasimm3/dota_project/issues/new?labels=enhancement&template=feature-request---.md

<!-- ROADMAP -->

## Roadmap

- [ ] Enable choice of output file type, currently the only available structure is .csv.
  - [X] Parquet
  - [ ] Json
  - [X] xlsx
- [ ] Import additional data from the API, including in-depth match stats.
- [ ] Build up a suite of PowerBI reports to get the ball rolling for the users.
- [X] Update Fact_Matches to pull from all staging files rather than the latest only.

See the [open issues](https://github.com/chasimm3/dota_project/issues) for a full list of proposed features (and known issues).

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- CONTRIBUTING -->

## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- LICENSE -->

## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- CONTACT -->

## Contact

Charlie Simmons - charlie.simmons92@gmail.com.com

Project Link: [https://github.com/chasimm3/dota_project](https://github.com/chasimm3/dota_project)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- ACKNOWLEDGMENTS -->

## Acknowledgments

* OpenDota for providing a free to use API.
* othneildrew for their indepth README examples and templates on GitHub: [https://github.com/othneildrew/Best-README-Template](https://github.com/othneildrew/Best-README-Template)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- MARKDOWN LINKS & IMAGES -->

<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->

[contributors-shield]: https://img.shields.io/github/contributors/chasimm3/dota_project.svg?style=for-the-badge
[contributors-url]: https://github.com/chasimm3/dota_project/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/chasimm3/dota_project.svg?style=for-the-badge
[forks-url]: https://github.com/chasimm3/dota_project/network/members
[stars-shield]: https://img.shields.io/github/stars/chasimm3/dota_project.svg?style=for-the-badge
[stars-url]: https://github.com/chasimm3/dota_project/stargazers
[issues-shield]: https://img.shields.io/github/issues/chasimm3/dota_project.svg?style=for-the-badge
[issues-url]: https://github.com/chasimm3/dota_project/issues
[license-shield]: https://img.shields.io/github/license/chasimm3/dota_project.svg?style=for-the-badge
[license-url]: https://github.com/chasimm3/dota_project/blob/master/LICENSE.txt
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://linkedin.com/in/charlie-simmons-25a25599/
[product-screenshot]: images/screenshot.png
