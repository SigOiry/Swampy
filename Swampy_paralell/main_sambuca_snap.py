# -*- coding: utf-8 -*-
"""
Created on Mon Feb  6 14:54:34 2017

@author: Marco
"""

import output_calculation
import define_outputs
import sambuca as sb




class main_sambuca:
    def __init__(self):
        pass

    def main_sambuca_func(self,observed_rrs,observed_rrs_width, observed_rrs_height, sensor_xml_path, siop_xml_path, par_xml_path, above_rrs_flag, shallow_flag, error_name, opt_met, relaxed):
        #self.observed_rrs=observed_rrs
        #self.observed_rrs_width=observed_rrs_width
        #self.observed_rrs_height=observed_rrs_height
        #print (observed_rrs.shape)
        image_info={}
        image_info['observed_rrs_width']=observed_rrs_width
        image_info['observed_rrs_height']=observed_rrs_height
        #image_info['base_path'] = 'C:\\Progetti\\sambuca_project\\input_data\\'

        [observed_rrs,image_info]=input_sensor_filter.input_sensor_filter(sensor_xml_path,observed_rrs,image_info, Rrs = above_rrs_flag)
        #self.p0_rand, self.p0_mid, self.num_params,self.pmin, self.pmax, self.p_bounds, self.awater,  self.aphy_star, self.substrates,  self.substrate_names]=input_parameters.sam_par(parameters_path,substrates_path)
        [siop, envmeta]=input_parameters.sam_par(siop_xml_path, par_xml_path)
        [wavelengths, siop, image_info, fixed_parameters, result_recorder, objective]=input_prepare.input_prepare(siop, envmeta,
                                                                                                           image_info, error_name)



        #if __name__=='main_sambuca_snap':



    #pool=None
        result_recorder=output_calculation.output_calculation(observed_rrs, objective, siop,
                                                                        result_recorder, image_info, opt_met, relaxed, shallow = shallow_flag)
        [closed_rrs, chl, cdom, nap, depth, nit, kd, sdi, sub1_frac, sub2_frac, sub3_frac, \
         error_f, total_abun, sub1_norm, sub2_norm, sub3_norm, rgbimg, r_sub]=define_outputs.output_suite(result_recorder, image_info)
        return depth, sdi, kd, error_f, r_sub, sub1_frac, sub2_frac, sub3_frac, nit

    def main_sambuca_func_simpl(self,observed_rrs, objective, observed_rrs_width, observed_rrs_height, sensor_filter, nedr, siop, fixed_parameters, shallow_flag, error_name, opt_met, relaxed, free_cpu=0, bathy=None, bathy_tolerance=None, bathy_exposed_mask=None, optimize_initial_guesses=False, use_five_initial_guesses=False, initial_guess_debug=False, fully_relaxed=False):
        image_info={}
        image_info['observed_rrs_width']=observed_rrs_width
        image_info['observed_rrs_height']=observed_rrs_height
        image_info['sensor_filter']=sensor_filter
        image_info['nedr']=nedr
        if bathy is not None:
            image_info['bathymetry'] = bathy
        if bathy_tolerance is not None:
            image_info['bathy_tolerance'] = bathy_tolerance
        if bathy_exposed_mask is not None:
            image_info['bathymetry_exposed_mask'] = bathy_exposed_mask

        result_recorder = sb.ArrayResultWriter(
            observed_rrs_height,
            observed_rrs_width,
            sensor_filter,
            nedr,
            fixed_parameters,
            store_initial_guesses=initial_guess_debug)

        result_recorder=output_calculation.output_calculation(observed_rrs, objective, siop,
                                                              result_recorder, image_info, opt_met, relaxed, shallow = shallow_flag, free_cpu=free_cpu,
                                                              optimize_initial_guesses=optimize_initial_guesses,
                                                              use_five_initial_guesses=use_five_initial_guesses,
                                                              fully_relaxed=fully_relaxed)
        [closed_rrs, chl, cdom, nap, depth,nit, kd, sdi, \
        sub1_frac, sub2_frac, sub3_frac, error_f, total_abun, sub1_norm,\
        sub2_norm, sub3_norm, rgbimg, r_sub]=define_outputs.output_suite(result_recorder, image_info)

        return closed_rrs, chl, cdom, nap, depth,nit, kd, sdi, \
        sub1_frac, sub2_frac, sub3_frac, error_f, total_abun, sub1_norm,\
        sub2_norm, sub3_norm,  r_sub, result_recorder.initial_guess_stack
