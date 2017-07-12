"""
lanier4@illinois.edu

"""
import os
import filecmp
import time

DC_options_dict = {
                'run_pasted_gene_list'                      : 'pasted_gene_list',
                'run_samples_clustering_pipeline'           : 'samples_clustering_pipeline',
                'run_gene_prioritization_pipeline_pearson'  : 'gene_prioritization_pipeline_pearson',
                'run_gene_prioritization_pipeline_t_test'   : 'gene_prioritization_pipeline_t_test',
                'run_geneset_characterization_pipeline'     : 'geneset_characterization_pipeline'}

verify_root_dir = '../data/verification'
results_dir = './run_dir/results'

def run_all_BENCHMARKs_and_TESTs():
    """ run the make file targes for all yaml files and compre the results with their verification files """
    print('\n\nBegin Verification Testing:\t\t', time.strftime("%a, %b %d, %Y at %H:%M:%S", time.localtime()))
    directory_methods_dict = {v: k for k, v in (DC_options_dict).items()}
    verification_directory_list = sorted(directory_methods_dict.keys())

    for test_directory in verification_directory_list:
        t0 = time.time()
        verification_directory = os.path.join(verify_root_dir, test_directory)
        verification_method = 'make' + ' ' + directory_methods_dict[test_directory]
        print('\n\n\n\tRun Method:', verification_method, '\n', verification_directory)

        os.system(verification_method)
        python_file_compare(verification_directory, results_dir)
        print('remove result files:')
        for tmp_file_name in os.listdir(results_dir):
            if os.path.isfile(os.path.join(results_dir, tmp_file_name)):
                os.remove(os.path.join(results_dir, tmp_file_name))
        # os.system("make clean_dir_recursively create_run_dir copy_run_files")


def python_file_compare(verif_dir, results_dir):
    t0 = time.time()
    nix_dir, pipeline_name = os.path.split(verif_dir)
    match, mismatch, errs = filecmp.cmpfiles(results_dir, verif_dir, os.listdir(verif_dir))
    tt = '%0.3f'%(time.time() - t0)
    if len(errs) > 0:
        print('\n\t',tt,'\t', pipeline_name, 'test: FAIL')
        print('Errors:')
        for e in errs:
            print(e)
    if len(mismatch) > 0:
        print('\n\t',tt,'\t', pipeline_name, 'test: not passed')
        print('Mismatch:')
        for mm in mismatch:
            print(mm)
    if len(match) > 0:
        print('\n\t',tt,'\t', pipeline_name, 'test: PASS')
        print('Matched:')
        for m in match:
            print(m)



def main():
    try:
        print('environment setup:')
        os.system('make env_setup')
    except:
        pass

    run_all_BENCHMARKs_and_TESTs()
    print('\n')

if __name__ == "__main__":
    main()