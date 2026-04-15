from os.path import join
import os.path
#import rasterio
import sambuca as sb
import sambuca_core as sbc
#import nibabel as nib
import numpy as np
import numpy.ma as ma
import xmltodict
from tkinter import *
import sys
import sambuca as sb
import sambuca_core as sbc
np.set_printoptions(threshold=sys.maxsize)

def read_sensor_filter(sensor_xml_path):
	if __name__=='create_input':
		#open the xml file
		xml=open(sensor_xml_path, 'rb')
		my_dict=xmltodict.parse(xml.read())
		nedrw=my_dict['root']['nedr']['item'][0]['item']
		nedrs=my_dict['root']['nedr']['item'][1]['item']
		#we map strings into float
		nedrw_m=np.array(list(map(float, nedrw)))
		nedrs_m=np.array(list(map(float, nedrs)))
		proc_bands=np.ones((len(nedrw))).astype('int')
		mask_filter_bands = np.array(proc_bands, dtype = bool)

		#nw_original is the nunmber of central wavelenghts
		nw_original=len(nedrw)

		#we create the tuple for nedr
		nedr=tuple([nedrw_m, nedrs_m])
		#nw is the nunmber of central wavelenghts
		nw=len(nedrw)

		#in sfw we have the wlens of the sesnsor filters
		sfw=my_dict['root']['sensor_filter']['item'][0]['item']
		#in sf_dict we have the spectra of the filters
		sf_dict=my_dict['root']['sensor_filter']['item'][1]['item']

		sf=[] #intialize the list for the sensor spectra
		#we append to this list the spectra, mapping strings into float
		for i in range(nw_original):
			sf.append(np.array(list(map(float,sf_dict[i]['item']))))
		sfs=np.array(sf) #the array with the filters spectra
		#filter bands
		fs=sfs[mask_filter_bands]

		sfwm=np.array(list(map(float, sfw)))
		#we create the tuple for the sensor filter
		sensor_filter=tuple([sfwm, sfs])

		return sensor_filter, nedr

def read_sensor_filter_gui(sensor_xml_path,nbands):
	if __name__=='create_input':
		xml=open(sensor_xml_path, 'rb')
		my_dict=xmltodict.parse(xml.read())
		nedrw=my_dict['root']['nedr']['item'][0]['item']
		nedrs=my_dict['root']['nedr']['item'][1]['item']
		image_bands=['NULL']
		#we map strings into float
		#TODO check length of nedrw and nedrs
		nedrw_m=np.array(list(map(float, nedrw)))
		nedrs_m=np.array(list(map(float, nedrs)))
		#nw_original is the nunmber of central wavelenghts
		nw_original=len(nedrw)
		proc_bands=np.ones((len(nedrw))).astype('int')
		mask_filter_bands = np.array(proc_bands, dtype = bool)
		#we create the tuple for nedr
		nedrw_m = nedrw_m[mask_filter_bands]
		nedrs_m = nedrs_m[mask_filter_bands]
		nedr=tuple([nedrw_m, nedrs_m])
		#nw is the nunmber of central wavelenghts
		nw=len(nedrw)
		#in sfw we have the wlens of the sesnsor filters
		sfw=my_dict['root']['sensor_filter']['item'][0]['item']
		#in sf_dict we have the spectra of the filters
		sf_dict=my_dict['root']['sensor_filter']['item'][1]['item']
		sf=[] #intialize the list for the sensor spectra
		#we append to this list the spectra, mapping strings into float
		#TODO check length of sf_dict (it should be nw)
		#TODO check length of sf_dict[i] (it should be equal to len(sfw))
		for i in range(nw_original):
			sf.append(np.array(list(map(float,sf_dict[i]['item']))))
		#filter bands
		band_namaes_2=[]
		#for (b_n, masked) in zip(band_names, ):
		if nbands<4:
			root=Tk()
			root.title("Error")
			Label(root, text="SWAMpy requires at least four valid bands to run").grid(row=0, sticky=W)
			Button (root, text='Ok', command=root.destroy).grid(row=1, sticky=W)
			root.mainloop()
		sfwm=np.array(list(map(float, sfw)))
		#we create the tuple for the sensor filter
		sfs=np.array(sf)
		#sfs=sfs[mask_filter_bands]
		sensor_filter=tuple([sfwm, sfs])
		return sensor_filter, nedr



def read_siop(siop_xml_path, p_min_list, p_max_list):
	if __name__=='create_input':
		#we read and parse the .xml file chosen by the user
		xml=open(siop_xml_path, 'rb')
		siop_dict=xmltodict.parse(xml.read())
		#we read the a_water spectrum
		a0=siop_dict['root']['a_water']['item'][0]['item']
		a1=siop_dict['root']['a_water']['item'][1]['item']
		#we map the strings into float
		a0m=np.array(list(map(float, a0)))
		a1m=np.array(list(map(float, a1)))
		#we create the tuple
		awater=tuple([a0m, a1m])
		#now for aphy_star
		ap0=siop_dict['root']['a_ph_star']['item'][0]['item']
		ap1=siop_dict['root']['a_ph_star']['item'][1]['item']
		ap0m=np.array(list(map(float, ap0)))
		ap1m=np.array(list(map(float, ap1)))
		aphy_star=tuple([ap0m, ap1m])
		#we initialize bb_nap_slope
		bb_nap_slope=None
		#if in the .xml bb_nap_slop is not None, we allocate the value in the variable
		if type(siop_dict['root']['bb_nap_slope'])==str:
			bb_nap_slope=float(siop_dict['root']['bb_nap_slope'])
		#we read the first subtrate spectrum
		sw1=siop_dict['root']['substrates']['item'][0]['item'][0]['item']
		ss1=siop_dict['root']['substrates']['item'][0]['item'][1]['item']
		#we map the string into float
		sw1m=np.array(list(map(float, sw1)))
		ss1m=np.array(list(map(float, ss1)))
		sub_1=tuple([sw1m,ss1m])
		#same steps for the second subtrate
		sw2=siop_dict['root']['substrates']['item'][1]['item'][0]['item']
		ss2=siop_dict['root']['substrates']['item'][1]['item'][1]['item']
		sw2m=np.array(list(map(float, sw2)))
		ss2m=np.array(list(map(float, ss2)))
		sub_2=tuple([sw2m,ss2m])
		#same steps for the third subtrate
		sw3=siop_dict['root']['substrates']['item'][2]['item'][0]['item']
		ss3=siop_dict['root']['substrates']['item'][2]['item'][1]['item']
		sw3m=np.array(list(map(float, sw3)))
		ss3m=np.array(list(map(float, ss3)))
		sub_3=tuple([sw3m,ss3m])
		#we create a list with the three substrates
		substrates=[sub_1, sub_2, sub_3]
		#we create the lists with the parameters values
		# the values of the free_parameters are taken for the lists created from the .xml file
		p_min = sb.FreeParameters(
			chl=p_min_list[0],               # Concentration of chlorophyll (algal organic particulates)
			cdom=p_min_list[1],            # Concentration of coloured dissolved organic particulates
			nap=p_min_list[2],                # Concentration of non-algal particulates
			depth=p_min_list[3],              # Water column depth
			sub1_frac=p_min_list[4],
			sub2_frac=p_min_list[5],
			sub3_frac=p_min_list[6])
		p_max = sb.FreeParameters(
			chl=p_max_list[0],
			cdom=p_max_list[1],
			nap=p_max_list[2],
			depth=p_max_list[3],
			sub1_frac=p_max_list[4],
			sub2_frac=p_max_list[5],
			sub3_frac=p_max_list[6])
		# repackage p_min and p_max into the tuple of (min,max) pairs expected by our objective function,
		# and by the minimisation methods that support bounds
		p_bounds = tuple(zip(p_min, p_max))
		#we allocate the constant values from the .xml files
		siop = {'a_water': awater, 'a_ph_star': aphy_star, 'substrates': substrates, 'substrate_names': siop_dict['root']['substrate_names']['item'],\
			'a_cdom_slope': float(siop_dict['root']['a_cdom_slope']),\
			'a_nap_slope': float(siop_dict['root']['a_nap_slope']),\
			'bb_ph_slope': float(siop_dict['root']['bb_ph_slope']),\
			'bb_nap_slope': bb_nap_slope,\
			'lambda0cdom': float(siop_dict['root']['lambda0cdom']),\
			'lambda0nap': float(siop_dict['root']['lambda0nap']),\
			'lambda0x': float(siop_dict['root']['lambda0x']),\
			'x_ph_lambda0x': float(siop_dict['root']['x_ph_lambda0x']),\
			'x_nap_lambda0x': float(siop_dict['root']['x_nap_lambda0x']),\
			'a_cdom_lambda0cdom': float(siop_dict['root']['a_cdom_lambda0cdom']),\
			'a_nap_lambda0nap': float(siop_dict['root']['a_nap_lambda0nap']),\
			'bb_lambda_ref': float(siop_dict['root']['bb_lambda_ref']),\
			'water_refractive_index': float(siop_dict['root']['water_refractive_index']),\
			'p_min': p_min, 'p_max': p_max, 'p_bounds': p_bounds}
		#we allocate the constant values from the .xml files
		envmeta = {'theta_air': 45.,\
			'off_nadir': 0., 'q_factor': np.pi}
		return siop, envmeta




def prepare_input(siop, envmeta, image_info, error_name):
	a_water=siop['a_water']
	a_ph_star=siop['a_ph_star']
	substrates=siop['substrates']
	substrate_names=siop['substrate_names']
	sensor_filter=image_info['sensor_filter']
	nedr=image_info['nedr']
	wavelengths = sbc.spectra_find_common_wavelengths(a_water, a_ph_star, *substrates)
	#Use the common wavelengths to mask the inputs:
	a_water = sbc.spectra_apply_wavelength_mask(a_water, wavelengths)
	a_ph_star = sbc.spectra_apply_wavelength_mask(a_ph_star, wavelengths)
	for i, substrate in enumerate(substrates):
		substrates[i] = sbc.spectra_apply_wavelength_mask(substrate, wavelengths)
	print('awater wavelength range: min: {0}  max: {1}'.format(min(a_water[0]), max(a_water[0])))
	print('a_ph_star wavelength range: min: {0}  max: {1}'.format(min(a_ph_star[0]), max(a_ph_star[0])))
	for substrate_name, substrate in zip(substrate_names, substrates):
		print('{0} wavelength range: min: {1}  max: {2}'.format(substrate_name, min(substrate[0]), max(substrate[0])))
	"""Truncate the sensor filter to match the common wavelength range
	It remains to be seen whether this is the best approach, but it works for this demo. An alternative approach would be to truncate the entire band for any band that falls outside the common wavelength range.
	If this approach, or something based on it, is valid, then this should be moved into a sambuca_core function with appropriate unit tests."""
	wav_min=ma.max([ma.min(a_water[0]), ma.min(a_ph_star[0]), ma.min(sensor_filter[0])])
	wav_max=ma.min([ma.max(a_water[0]), ma.max(a_ph_star[0]), ma.max(sensor_filter[0])])
	#we mask the sensor filter, chosing only the common wavelength
	filter_mask = ma.where((sensor_filter[0] >= wavelengths.min()) & (sensor_filter[0] <= wavelengths.max()))
	sensor_filter = sensor_filter[0][filter_mask], sensor_filter[1][:,filter_mask]
	list_sen=[]
	#create a list for the sensor filter, in order to use this in the tuple
	for i_sen, elem in enumerate(sensor_filter[1]):
		list_sen.append(elem[0])
	list_sen=np.array(list_sen)
	sensor_filter=sensor_filter[0], list_sen
	#we mask the substrates
	for i, substrate in enumerate(substrates):
		substrates[i]=substrate[0][ma.argmin(ma.abs(wav_min-substrate[0])): ma.argmin(ma.abs(wav_max-substrate[0]))+1],\
			 substrate[1][ma.argmin(ma.abs(wav_min-substrate[0])): ma.argmin(ma.abs(wav_max-substrate[0]))+1]
	filter_mask_550=ma.where(sensor_filter[0]==550.)[0][0]
	index=[]
	index.append(filter_mask_550)
	#now only the wavelenght of interest (where the sensor filter is not 0 for at least 1 band)
	for elements in sensor_filter[1]:
		for (ind_wav, sen) in enumerate(elements):
			if sen>0. and ind_wav not in index:
				index.append(ind_wav)
	index=sorted(index)
	#we now use the index to choose only the band of the sensor filter
	sensor_filter=sensor_filter[0][index], sensor_filter[1][:,index]
	a_water=a_water[0], a_water[1][index]
	a_ph_star=a_ph_star[0], a_ph_star[1][index]
	wavelengths=wavelengths[index]
	for i, substrate in enumerate(substrates):
		substrates[i] = substrate[0][index],substrate[1][index]
	#create the set of fixed parameters
	fixed_parameters = sb.create_fixed_parameter_set(
		wavelengths=wavelengths,
		a_water=a_water,
		a_ph_star=a_ph_star,
		substrates=substrates,
		sub1_frac=None,
		sub2_frac=None,
		sub3_frac=None,
		chl=None,
		cdom=None,
		nap=None,
		depth=None,
		a_cdom_slope=siop['a_cdom_slope'],
		a_nap_slope=siop['a_nap_slope'],
		bb_ph_slope=siop['bb_ph_slope'],
		bb_nap_slope=siop['bb_nap_slope'],
		lambda0cdom=siop['lambda0cdom'],
		lambda0nap=siop['lambda0nap'],
		lambda0x=siop['lambda0x'],
		x_ph_lambda0x=siop['x_ph_lambda0x'],
		x_nap_lambda0x=siop['x_nap_lambda0x'],
		a_cdom_lambda0cdom=siop['a_cdom_lambda0cdom'],
		a_nap_lambda0nap=siop['a_nap_lambda0nap'],
		bb_lambda_ref=siop['bb_lambda_ref'],
		water_refractive_index=siop['water_refractive_index'],
		theta_air=envmeta['theta_air'],
		off_nadir=envmeta['off_nadir'],
		q_factor=envmeta['q_factor']
	)
	error_dict={'alpha':sb.distance_alpha, 'alpha_f': sb.distance_alpha_f, 'lsq':sb.distance_lsq, 'f':sb.distance_f}
	objective = sb.SciPyObjective(sensor_filter, fixed_parameters, error_name.lower(), nedr=nedr)
	#store the spectra into the siop dict
	siop['a_water']=a_water
	siop['a_ph_star']=a_ph_star
	siop['substrates']=substrates
	siop['substrate_names']=substrate_names
	image_info['sensor_filter']=sensor_filter
	image_info['nedr']=nedr
	return wavelengths, siop, image_info, fixed_parameters, objective


