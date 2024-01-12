[![INFORMS Journal on Computing Logo](https://INFORMSJoC.github.io/logos/INFORMS_Journal_on_Computing_Header.jpg)](https://pubsonline.informs.org/journal/ijoc)

# Computing optimality certificates for convex mixed-integer nonlinear problems

This archive is distributed in association with the [INFORMS Journal on
Computing](https://pubsonline.informs.org/journal/ijoc) under the [MIT License](LICENSE).

The software and data in this repository are a snapshot of the software and data
that were used in the research reported on in the paper
[Computing optimality certificates for convex mixed-integer nonlinear problems](https://doi.org/10.1287/ijoc.2022.0099) by K. Halbig, L. Hümbs, F. Rösel, L. Schewe, D. Weninger.

## Cite

To cite the contents of this repository, please cite both the paper and this repo, using their respective DOIs.

https://doi.org/10.1287/ijoc.2022.0099

https://doi.org/10.1287/ijoc.2022.0099.cd

Below is the BibTex for citing this snapshot of the repository.

```
@article{Halbig2023OptCert,
  author =        {K. Halbig, L. Hümbs, F. Rösel, L. Schewe, D. Weninger},
  publisher =     {INFORMS Journal on Computing},
  title =         {{Computing optimality certificates for convex mixed-integer nonlinear problems}},
  year =          {2023},
  doi =           {10.1287/ijoc.2022.0099.cd},
  url =           {https://github.com/INFORMSJoC/2022.0099},
}
```

## Description

The goal of this software is to compute (the size of) optimality certificates for convex mixed-integer nonlinear problems.

## Building

In order to run the code you need an installation with a valid license of [Gurobi](https://www.gurobi.com/).

No further installation is necessary. The code can directly get called, for example,
```
python3 compute_certificate.py -f ../data/MIPLIB_Instances/cbs-cta
```
Further help and an overview of all parameters is provided by
```
python3 compute_certificate.py --help
```

Remark: The present code was tested on machines running Ubuntu 18, Ubuntu 22, and AlmaLinux 8, each with english language settings.
If you encounter any problems, consider switching to one of these operating systems if you are using a different one.

## Replicating

All used instances are given in directory `data`. These instances are copied from https://miplib.zib.de/ or https://www.minlplib.org/, and converted to the `.lp` format.

To replicate the results in the paper, run

```
python3 compute_certificate.py -fbrl --numeric 0 --memory_limit 30000 --time_limit 84600 ../data/folder/instancename
```
for each instance. The results are written to folder `src/Certificates`.