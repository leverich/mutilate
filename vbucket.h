/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 NorthScale, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

/*! \mainpage libvbucket
 *
 * \section intro_sec Introduction
 *
 * libvbucket helps you understand and make use of vbuckets for
 * scaling kv services.
 *
 * \section docs_sec API Documentation
 *
 * Jump right into <a href="modules.html">the modules docs</a> to get started.
 */

/**
 * VBucket Utility Library.
 *
 * \defgroup CD Creation and Destruction
 * \defgroup cfg Config Access
 * \defgroup cfgcmp Config Comparison
 * \defgroup vb VBucket Access
 * \defgroup err Error handling
 */

#ifndef LIBVBUCKET_VBUCKET_H
#define LIBVBUCKET_VBUCKET_H 1

#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

    struct vbucket_config_st;

    /**
     * Opaque config representation.
     */
    typedef struct vbucket_config_st* VBUCKET_CONFIG_HANDLE;

    /**
     * \addtogroup cfgcmp
     * @{
     */

    /**
     * Difference between two vbucket configs.
     */
    typedef struct {
        /**
         * NULL-terminated list of server names that were added.
         */
        char **servers_added;
        /**
         * NULL-terminated list of server names that were removed.
         */
        char **servers_removed;
        /**
         * Number of vbuckets that changed.  -1 if the total number changed
         */
        int n_vb_changes;
        /**
         * True if the sequence of servers changed.
         */
        bool sequence_changed;
    } VBUCKET_CONFIG_DIFF;

    /**
     * @}
     */

    /**
     * \addtogroup CD
     *  @{
     */

    /**
     * Create an instance of vbucket config from a file.
     *
     * @param filename the vbucket config to parse
     */
    
    VBUCKET_CONFIG_HANDLE vbucket_config_parse_file(const char *filename);

    /**
     * Create an instance of vbucket config from a string.
     *
     * @param data a vbucket config string.
     */
    
    VBUCKET_CONFIG_HANDLE vbucket_config_parse_string(const char *data);
    
     /**
     * Create a cloned instance of a vbucket config. Any modifications to
     * the clone will not be transfered to the original copy. 
     *
     * @param vb vbucket config to clone from
     */
    
    VBUCKET_CONFIG_HANDLE vbucket_config_clone(VBUCKET_CONFIG_HANDLE vb);

    /**
     * Destroy a vbucket config.
     *
     * @param h the vbucket config handle
     */
    
    void vbucket_config_destroy(VBUCKET_CONFIG_HANDLE h);

    /**
     * @}
     */

    /**
     * \addtogroup err
     *  @{
     */

    /**
     * Get the most recent vbucket error.
     *
     * This is currently not threadsafe.
     */
    
    const char *vbucket_get_error(void);

    /**
     * Tell libvbucket it told you the wrong server ID.
     *
     * This will cause libvbucket to do whatever is necessary to try
     * to figure out a better answer.
     *
     * @param h the vbucket config handle.
     * @param vbucket the vbucket ID
     * @param wrongserver the incorrect server ID
     *
     * @return the correct server ID
     */
    
    int vbucket_found_incorrect_master(VBUCKET_CONFIG_HANDLE h,
                                       int vbucket,
                                       int wrongserver);

    /**
     * @}
     */

    /**
     * \addtogroup cfg
     * @{
     */

    /**
     * Get the number of replicas configured for each vbucket.
     */
    
    int vbucket_config_get_num_replicas(VBUCKET_CONFIG_HANDLE h);

    /**
     * Get the total number of vbuckets;
     */
    
    int vbucket_config_get_num_vbuckets(VBUCKET_CONFIG_HANDLE h);

    /**
     * Get the total number of known servers.
     */
    
    int vbucket_config_get_num_servers(VBUCKET_CONFIG_HANDLE h);

    /**
     * Get the optional SASL user.
     *
     * @return a string or NULL.
     */
    
    const char *vbucket_config_get_user(VBUCKET_CONFIG_HANDLE h);

    /**
     * Get the optional SASL password.
     *
     * @return a string or NULL.
     */
    
    const char *vbucket_config_get_password(VBUCKET_CONFIG_HANDLE h);

    /**
     * Get the server at the given index.
     *
     * @return a string in the form of hostname:port
     */
    
    const char *vbucket_config_get_server(VBUCKET_CONFIG_HANDLE h, int i);

    /**
     * @}
     */

    /**
     * \addtogroup vb
     * @{
     */

    /**
     * Get the vbucket number for the given key.
     *
     * @param h the vbucket config
     * @param key pointer to the beginning of the key
     * @param nkey the size of the key
     *
     * @return a key
     */
    
    int vbucket_get_vbucket_by_key(VBUCKET_CONFIG_HANDLE h,
                                   const void *key, size_t nkey);

    /**
     * Get the master server for the given vbucket.
     *
     * @param h the vbucket config
     * @param id the vbucket identifier
     *
     * @return the server index
     */
    
    int vbucket_get_master(VBUCKET_CONFIG_HANDLE h, int id);

    /**
     * Get a given replica for a vbucket.
     *
     * @param h the vbucket config
     * @param id the vbucket id
     * @param n the replica number
     *
     * @return the server ID
     */
    
    int vbucket_get_replica(VBUCKET_CONFIG_HANDLE h, int id, int n);

    /**
     * @}
     */

    /**
     * \addtogroup cfgcmp
     * @{
     */

    /**
     * Compare two vbucket handles.
     *
     * @param from the source vbucket config
     * @param to the destination vbucket config
     *
     * @return what changed between the "from" config and the "to" config
     */
    
    VBUCKET_CONFIG_DIFF* vbucket_compare(VBUCKET_CONFIG_HANDLE from,
                                         VBUCKET_CONFIG_HANDLE to);

    /**
     * Free a vbucket diff.
     *
     * @param diff the diff to free
     */
    
    void vbucket_free_diff(VBUCKET_CONFIG_DIFF *diff);

    /**
     * @}
     */

#ifdef __cplusplus
}
#endif

#endif
