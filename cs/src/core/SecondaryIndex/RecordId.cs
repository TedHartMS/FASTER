// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Encapsulates the address and version of a record in the log
    /// </summary>
    public struct RecordId
    {
        private long word;

        internal RecordId(long address, RecordInfo recordInfo) : this(address, recordInfo.Version) { }

        internal RecordId(long address, int version)
        {
            this.word = default;
            this.Address = address;
            this.Version = version;
        }

        /// <summary>
        /// The version of the record
        /// </summary>
        public int Version
        {
            get
            {
                return (int)(((word & RecordInfo.kVersionMaskInWord) >> RecordInfo.kVersionShiftInWord) & RecordInfo.kVersionMaskInInteger);
            }
            set
            {
                word &= ~RecordInfo.kVersionMaskInWord;
                word |= ((value & RecordInfo.kVersionMaskInInteger) << RecordInfo.kVersionShiftInWord);
            }
        }

        /// <summary>
        /// The logical address of the record
        /// </summary>
        public long Address
        {
            get
            {
                return (word & RecordInfo.kPreviousAddressMask);
            }
            set
            {
                word &= ~RecordInfo.kPreviousAddressMask;
                word |= (value & RecordInfo.kPreviousAddressMask);
            }
        }

        /// <summary>
        /// Check that the passed record address and version matches this RecordInfo
        /// </summary>
        public bool Equals(long address, int version) => this.Address == address && this.Version == version;

        /// <summary>
        /// Whether this is a default instance of RecordId
        /// </summary>
        public bool IsDefault => this.Address == Constants.kInvalidAddress;

        /// <inheritdoc/>
        public override string ToString() => $"address {this.Address}, version {this.Version}";
    }
}
