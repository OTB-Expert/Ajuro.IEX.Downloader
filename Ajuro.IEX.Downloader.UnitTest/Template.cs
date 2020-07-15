using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ajuro.IEX.Downloader.UnitTest
{
    public class AjuroTemplate
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public int Version { get; set; }
        public long BasedOn { get; set; }
        public long CreatedBy { get; set; }
        public string Tags { get; set; }
        public string Headline { get; set; }
        public string Description { get; set; }
        public string Template { get; set; }
        public string Json { get; set; }
        public DateTime Timestamp { get; set; }
        public AjuroTemplate()
        {
        }
        public AjuroTemplate(long id, string name)
        {
                this.Id = id;
            this.Name = name;
        }
    }

	/*
	Test Cases

	
	 */
}
