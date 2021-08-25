namespace SQLQueryStress
{
    partial class Options
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
			this.IOStatistics_checkBox = new System.Windows.Forms.CheckBox();
			this.timeStatistics_checkBox = new System.Windows.Forms.CheckBox();
			this.label1 = new System.Windows.Forms.Label();
			this.label2 = new System.Windows.Forms.Label();
			this.connectionPooling_checkBox = new System.Windows.Forms.CheckBox();
			this.clientDataRetrieval_checkBox = new System.Windows.Forms.CheckBox();
			this.ok_button = new System.Windows.Forms.Button();
			this.cancel_button = new System.Windows.Forms.Button();
			this.groupBox1 = new System.Windows.Forms.GroupBox();
			this.connectionTimeout_numericUpDown = new System.Windows.Forms.NumericUpDown();
			this.groupBox2 = new System.Windows.Forms.GroupBox();
			this.killQueriesOnCancel_checkBox = new System.Windows.Forms.CheckBox();
			this.commandTimeout_numericUpDown = new System.Windows.Forms.NumericUpDown();
			this.dataStatistics_checkBox = new System.Windows.Forms.CheckBox();
			this.groupBox1.SuspendLayout();
			((System.ComponentModel.ISupportInitialize)(this.connectionTimeout_numericUpDown)).BeginInit();
			this.groupBox2.SuspendLayout();
			((System.ComponentModel.ISupportInitialize)(this.commandTimeout_numericUpDown)).BeginInit();
			this.SuspendLayout();
			// 
			// IOStatistics_checkBox
			// 
			this.IOStatistics_checkBox.AutoSize = true;
			this.IOStatistics_checkBox.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point);
			this.IOStatistics_checkBox.Location = new System.Drawing.Point(7, 67);
			this.IOStatistics_checkBox.Margin = new System.Windows.Forms.Padding(4, 3, 4, 3);
			this.IOStatistics_checkBox.Name = "IOStatistics_checkBox";
			this.IOStatistics_checkBox.Size = new System.Drawing.Size(144, 17);
			this.IOStatistics_checkBox.TabIndex = 1;
			this.IOStatistics_checkBox.Text = "Collect I/O Statistics";
			this.IOStatistics_checkBox.UseVisualStyleBackColor = true;
			// 
			// timeStatistics_checkBox
			// 
			this.timeStatistics_checkBox.AutoSize = true;
			this.timeStatistics_checkBox.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point);
			this.timeStatistics_checkBox.Location = new System.Drawing.Point(7, 94);
			this.timeStatistics_checkBox.Margin = new System.Windows.Forms.Padding(4, 3, 4, 3);
			this.timeStatistics_checkBox.Name = "timeStatistics_checkBox";
			this.timeStatistics_checkBox.Size = new System.Drawing.Size(152, 17);
			this.timeStatistics_checkBox.TabIndex = 2;
			this.timeStatistics_checkBox.Text = "Collect Time Statistics";
			this.timeStatistics_checkBox.UseVisualStyleBackColor = true;
			// 
			// label1
			// 
			this.label1.AutoSize = true;
			this.label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point);
			this.label1.Location = new System.Drawing.Point(4, 20);
			this.label1.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
			this.label1.Name = "label1";
			this.label1.Size = new System.Drawing.Size(120, 13);
			this.label1.TabIndex = 3;
			this.label1.Text = "Connection Timeout";
			// 
			// label2
			// 
			this.label2.AutoSize = true;
			this.label2.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point);
			this.label2.Location = new System.Drawing.Point(4, 20);
			this.label2.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
			this.label2.Name = "label2";
			this.label2.Size = new System.Drawing.Size(196, 13);
			this.label2.TabIndex = 5;
			this.label2.Text = "Command Timeout (0 = Unlimited)";
			// 
			// connectionPooling_checkBox
			// 
			this.connectionPooling_checkBox.AutoSize = true;
			this.connectionPooling_checkBox.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point);
			this.connectionPooling_checkBox.Location = new System.Drawing.Point(7, 67);
			this.connectionPooling_checkBox.Margin = new System.Windows.Forms.Padding(4, 3, 4, 3);
			this.connectionPooling_checkBox.Name = "connectionPooling_checkBox";
			this.connectionPooling_checkBox.Size = new System.Drawing.Size(179, 17);
			this.connectionPooling_checkBox.TabIndex = 1;
			this.connectionPooling_checkBox.Text = "Enable Connection Pooling";
			this.connectionPooling_checkBox.UseVisualStyleBackColor = true;
			// 
			// clientDataRetrieval_checkBox
			// 
			this.clientDataRetrieval_checkBox.AutoSize = true;
			this.clientDataRetrieval_checkBox.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point);
			this.clientDataRetrieval_checkBox.Location = new System.Drawing.Point(7, 148);
			this.clientDataRetrieval_checkBox.Margin = new System.Windows.Forms.Padding(4, 3, 4, 3);
			this.clientDataRetrieval_checkBox.Name = "clientDataRetrieval_checkBox";
			this.clientDataRetrieval_checkBox.Size = new System.Drawing.Size(195, 17);
			this.clientDataRetrieval_checkBox.TabIndex = 4;
			this.clientDataRetrieval_checkBox.Text = "Force Client Retrieval of Data";
			this.clientDataRetrieval_checkBox.UseVisualStyleBackColor = true;
			// 
			// ok_button
			// 
			this.ok_button.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Right)));
			this.ok_button.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point);
			this.ok_button.Location = new System.Drawing.Point(401, 219);
			this.ok_button.Margin = new System.Windows.Forms.Padding(4, 3, 4, 3);
			this.ok_button.Name = "ok_button";
			this.ok_button.Size = new System.Drawing.Size(88, 27);
			this.ok_button.TabIndex = 2;
			this.ok_button.Text = "OK";
			this.ok_button.UseVisualStyleBackColor = true;
			this.ok_button.Click += new System.EventHandler(this.ok_button_Click);
			// 
			// cancel_button
			// 
			this.cancel_button.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Right)));
			this.cancel_button.DialogResult = System.Windows.Forms.DialogResult.Cancel;
			this.cancel_button.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point);
			this.cancel_button.Location = new System.Drawing.Point(496, 219);
			this.cancel_button.Margin = new System.Windows.Forms.Padding(4, 3, 4, 3);
			this.cancel_button.Name = "cancel_button";
			this.cancel_button.Size = new System.Drawing.Size(88, 27);
			this.cancel_button.TabIndex = 3;
			this.cancel_button.Text = "Cancel";
			this.cancel_button.UseVisualStyleBackColor = true;
			this.cancel_button.Click += new System.EventHandler(this.cancel_button_Click);
			// 
			// groupBox1
			// 
			this.groupBox1.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left)));
			this.groupBox1.Controls.Add(this.connectionTimeout_numericUpDown);
			this.groupBox1.Controls.Add(this.label1);
			this.groupBox1.Controls.Add(this.connectionPooling_checkBox);
			this.groupBox1.Location = new System.Drawing.Point(14, 14);
			this.groupBox1.Margin = new System.Windows.Forms.Padding(4, 3, 4, 3);
			this.groupBox1.Name = "groupBox1";
			this.groupBox1.Padding = new System.Windows.Forms.Padding(4, 3, 4, 3);
			this.groupBox1.Size = new System.Drawing.Size(281, 196);
			this.groupBox1.TabIndex = 0;
			this.groupBox1.TabStop = false;
			this.groupBox1.Text = "Connection Options";
			// 
			// connectionTimeout_numericUpDown
			// 
			this.connectionTimeout_numericUpDown.Location = new System.Drawing.Point(7, 38);
			this.connectionTimeout_numericUpDown.Margin = new System.Windows.Forms.Padding(4, 3, 4, 3);
			this.connectionTimeout_numericUpDown.Maximum = new decimal(new int[] {
            60,
            0,
            0,
            0});
			this.connectionTimeout_numericUpDown.Minimum = new decimal(new int[] {
            1,
            0,
            0,
            0});
			this.connectionTimeout_numericUpDown.Name = "connectionTimeout_numericUpDown";
			this.connectionTimeout_numericUpDown.Size = new System.Drawing.Size(140, 23);
			this.connectionTimeout_numericUpDown.TabIndex = 0;
			this.connectionTimeout_numericUpDown.Value = new decimal(new int[] {
            1,
            0,
            0,
            0});
			// 
			// groupBox2
			// 
			this.groupBox2.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left)));
			this.groupBox2.Controls.Add(this.dataStatistics_checkBox);
			this.groupBox2.Controls.Add(this.killQueriesOnCancel_checkBox);
			this.groupBox2.Controls.Add(this.commandTimeout_numericUpDown);
			this.groupBox2.Controls.Add(this.label2);
			this.groupBox2.Controls.Add(this.IOStatistics_checkBox);
			this.groupBox2.Controls.Add(this.timeStatistics_checkBox);
			this.groupBox2.Controls.Add(this.clientDataRetrieval_checkBox);
			this.groupBox2.Location = new System.Drawing.Point(302, 14);
			this.groupBox2.Margin = new System.Windows.Forms.Padding(4, 3, 4, 3);
			this.groupBox2.Name = "groupBox2";
			this.groupBox2.Padding = new System.Windows.Forms.Padding(4, 3, 4, 3);
			this.groupBox2.Size = new System.Drawing.Size(281, 196);
			this.groupBox2.TabIndex = 1;
			this.groupBox2.TabStop = false;
			this.groupBox2.Text = "Command Options";
			// 
			// killQueriesOnCancel_checkBox
			// 
			this.killQueriesOnCancel_checkBox.AutoSize = true;
			this.killQueriesOnCancel_checkBox.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point);
			this.killQueriesOnCancel_checkBox.Location = new System.Drawing.Point(7, 175);
			this.killQueriesOnCancel_checkBox.Margin = new System.Windows.Forms.Padding(4, 3, 4, 3);
			this.killQueriesOnCancel_checkBox.Name = "killQueriesOnCancel_checkBox";
			this.killQueriesOnCancel_checkBox.Size = new System.Drawing.Size(151, 17);
			this.killQueriesOnCancel_checkBox.TabIndex = 5;
			this.killQueriesOnCancel_checkBox.Text = "Kill Queries on Cancel";
			this.killQueriesOnCancel_checkBox.UseVisualStyleBackColor = true;
			// 
			// commandTimeout_numericUpDown
			// 
			this.commandTimeout_numericUpDown.Location = new System.Drawing.Point(7, 38);
			this.commandTimeout_numericUpDown.Margin = new System.Windows.Forms.Padding(4, 3, 4, 3);
			this.commandTimeout_numericUpDown.Maximum = new decimal(new int[] {
            32767,
            0,
            0,
            0});
			this.commandTimeout_numericUpDown.Name = "commandTimeout_numericUpDown";
			this.commandTimeout_numericUpDown.Size = new System.Drawing.Size(140, 23);
			this.commandTimeout_numericUpDown.TabIndex = 0;
			this.commandTimeout_numericUpDown.Tag = "";
			// 
			// dataStatistics_checkBox
			// 
			this.dataStatistics_checkBox.AutoSize = true;
			this.dataStatistics_checkBox.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point);
			this.dataStatistics_checkBox.Location = new System.Drawing.Point(7, 121);
			this.dataStatistics_checkBox.Margin = new System.Windows.Forms.Padding(4, 3, 4, 3);
			this.dataStatistics_checkBox.Name = "dataStatistics_checkBox";
			this.dataStatistics_checkBox.Size = new System.Drawing.Size(180, 17);
			this.dataStatistics_checkBox.TabIndex = 3;
			this.dataStatistics_checkBox.Text = "Collect Data Size Statistics";
			this.dataStatistics_checkBox.UseVisualStyleBackColor = true;
			// 
			// Options
			// 
			this.AcceptButton = this.ok_button;
			this.AutoScaleDimensions = new System.Drawing.SizeF(7F, 15F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.CancelButton = this.cancel_button;
			this.ClientSize = new System.Drawing.Size(600, 256);
			this.Controls.Add(this.groupBox2);
			this.Controls.Add(this.groupBox1);
			this.Controls.Add(this.cancel_button);
			this.Controls.Add(this.ok_button);
			this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedDialog;
			this.Margin = new System.Windows.Forms.Padding(4, 3, 4, 3);
			this.MaximizeBox = false;
			this.MinimizeBox = false;
			this.Name = "Options";
			this.ShowIcon = false;
			this.ShowInTaskbar = false;
			this.StartPosition = System.Windows.Forms.FormStartPosition.CenterParent;
			this.Text = "Options";
			this.groupBox1.ResumeLayout(false);
			this.groupBox1.PerformLayout();
			((System.ComponentModel.ISupportInitialize)(this.connectionTimeout_numericUpDown)).EndInit();
			this.groupBox2.ResumeLayout(false);
			this.groupBox2.PerformLayout();
			((System.ComponentModel.ISupportInitialize)(this.commandTimeout_numericUpDown)).EndInit();
			this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.CheckBox IOStatistics_checkBox;
        private System.Windows.Forms.CheckBox timeStatistics_checkBox;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.CheckBox connectionPooling_checkBox;
        private System.Windows.Forms.CheckBox clientDataRetrieval_checkBox;
        private System.Windows.Forms.Button ok_button;
        private System.Windows.Forms.Button cancel_button;
        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.GroupBox groupBox2;
        private System.Windows.Forms.NumericUpDown connectionTimeout_numericUpDown;
        private System.Windows.Forms.NumericUpDown commandTimeout_numericUpDown;
        private System.Windows.Forms.CheckBox killQueriesOnCancel_checkBox;
		private System.Windows.Forms.CheckBox dataStatistics_checkBox;
	}
}