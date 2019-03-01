import os

names = ["Abkhaz_abk","Acoma_kjq","Alamblak_amp","Amele_aey","Apurina_apu","Arabic_Egyptian_arz","Arapesh_Mountain_ape","Asmat_tml","Bagirmi_bmi","Barasano_bsn","Basque_eus","Berber_Middle+Atlas_tzm","Burmese_mya","Burushaski_bsk","Canela-Kraho_ram","Chamorro_cha","Chukchi_ckt","Cree_Plains_crk","Daga_dgz","Dani_Lower+Grand+Valley_dni","English_eng","Fijian_fij","Finnish_fin","French_fra","Georgian_kat","German_deu","Gooniyandi_gni","Grebo_gry","Greek_Modern_ell","Greenlandic_West_kal","Guarani_gug","Hausa_hau","Hebrew_Modern_heb","Hindi_hin","Hixkaryana_hix","Hmong+Njua_hnj","Imonda_imn","Indonesian_ind","Jakaltek_jac","Japanese_jpn","Kannada_kan","Karok_kyh","Kayardild_gyd","Kewa_kew","Khalkha_khk","Khoekhoe_naq","Kiowa_kio","Koasati_cku","Korean_kor","Koyraboro+Senni_ses","Krongo_kgo","Kutenai_kut","Lakota_lkt","Lango_laj","Lavukaleve_lvk","Lezgian_lez","Luvale_lue","Makah_myh","Malagasy_plt","Mandarin_cmn","Mangarrayi_mpc","Mapudungun_arn","Maricopa_mrc","Martuthunira_vma","Maung_mph","Maybrat_ayz","Meithei_mni","Mixtec_Chalcatongo_mig","Ngiyambaa_wyb","Oneida_one","Oromo_Harar_hae","Otomi_Mezquital_ote","Paiwan_pwn","Persian_pes","Piraha_myp","Quechua_Imbabura_qvi","Rama_rma","Rapanui_rap","Russian_rus","Sango_sag","Sanuma_xsu","Slave_scs","Spanish_spa","Supyire_spp","Swahili_swh","Tagalog_tgl","Thai_tha","Tiwi_tiw","Tukang+Besi_bhq","Turkish_tur","Vietnamese_vie","Warao_wba","Wari_pav","Wichi_mzh","Wichita_wic","Yagua_yad","Yaqui_yaq","Yoruba_yor","Zoque_Copainala_zoc","Zulu_zul"]
first_level = ["fiction", "non-fiction", "conversation", "professional", "technical", "grammar_examples"]
second_level = ["written", "spoken"]

for name in names:
    os.mkdir('folders/' + name)
    for f in first_level:
        os.mkdir('folders/' + name + '/' + f)
        if f == "non-fiction" or f == "grammar_examples":
            for s in second_level:
                os.mkdir('folders/' + name + '/' + f+ '/' + s)