//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Ben Givertz first on 8/31/2021                                                                         //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_ORCBATCH_H
#define TUPLEX_ORCBATCH_H

namespace tuplex { namespace orc {

/*!
 * Interface for reading and writing to Orc batches from Tuplex fields
 */
class OrcBatch {
public:
    /*!
     * destructor must ensure all child batches are destroyed.
     */
    virtual ~OrcBatch() = default;

    /*!
     * sets the the data in a row of an Orc batch from a deserializer.
     * @param ds
     * @parm col
     * @param rowIndex
     */
    virtual void setData(tuplex::Deserializer &ds, uint64_t col, uint64_t rowIndex) = 0;

    /*!
     * sets the the data in a row of an Orc batch from a tuplex field.
     * @param field
     * @param row
     */
    virtual void setData(tuplex::Field field, uint64_t row) = 0;

    /*!
     * gets a tuplex field from an orc batch given the row.
     * @param row
     * @return Field
     */
    virtual tuplex::Field getField(uint64_t row) = 0;

    /*!
     * gets a tuplex field from an orc batch given a serializer.
     * @param serializer
     * @param row
     */
    virtual void getField(Serializer &serializer, uint64_t row) = 0;

    /*!
     * updates the orc batch used to read data from.
     * @param newBatch
     */
    virtual void setBatch(::orc::ColumnVectorBatch *newBatch) = 0;

    /*!
     * scale factor for resizing buffers
     * @return
     */
    static unsigned scaleFactor() { return 2; }

};

}}

#endif //TUPLEX_ORCBATCH_H
