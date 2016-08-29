from setuptools import setup, find_packages

setup(
    name='spt_ecmwf_autorapid_process',
    version='1.1.0',
    description='Python interface for RAPID (rapid-hub.org)',
    long_description='Computational framework to ingest ECMWF ensemble runoff forcasts;'
                     ' generate input for and run the RAPID (rapid-hub.org) program'
                     ' using HTCondor or Python\'s Multiprocessing; and upload to '
                     ' CKAN in order to be used by the Streamflow Prediction Tool (SPT).'
                     ' There is also an experimental option to use the AutoRoute program'
                     ' for flood inundation mapping.',
    keywords='ECMWF, RAPID, Flood Prediction, Streamflow Prediction Tool',
    author='Alan Dee Snow',
    author_email='alan.d.snow@usace.army.mil',
    url='https://github.com/erdc-cm/spt_ecmwf_autorapid_process',
    download_url='https://github.com/erdc-cm/spt_ecmwf_autorapid_process/archive/1.1.0.tar.gz',
    license='BSD 3-Clause',
    packages=find_packages(),
    install_requires=['numpy', 'netCDF4', 'RAPIDpy'],
    classifiers=[
                'Intended Audience :: Developers',
                'Intended Audience :: Science/Research',
                'Operating System :: OS Independent',
                'Programming Language :: Python',
                'Programming Language :: Python :: 2',
                'Programming Language :: Python :: 2.7',
                ],
)
